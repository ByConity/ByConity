#include "UserDefinedExternalFunctionFactory.h"

#include <filesystem>
#include <sys/un.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <Common/FieldVisitorToString.h>
#include <Common/escapeForFileName.h>
#include <Common/filesystemHelpers.h>

#include <Processors/Sources/SourceFromSingleChunk.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>

#include <Protos/UDF.pb.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <fmt/core.h>

#include "FormatPath.h"
#include "Proto.h"
#include "Serialization.h"
#include "SharedPath.h"
#include "VectorType.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int BAD_ARGUMENTS;
    extern const int EXTERNAL_LIBRARY_ERROR;
}

namespace
{

    struct UDFContext
    {
        std::atomic_uint32_t id; /* running sequence of RPC call id */
        std::string uds_path; /* path of unix domain socket */
        std::string path; /* path of user_defined */
        uint16_t base; /* RPC call base offset */

        /* stub to UDF manager */
        std::unique_ptr<brpc::Channel> brpc_channel;
        std::unique_ptr<UDF_Stub> stub;
        Poco::Logger * log;

        void createClient(const brpc::ChannelOptions * opts)
        {
            char rpc_path[PATH_MAX];

            rpc_uds_fmt(rpc_path, uds_path.c_str());
            brpc_channel = std::make_unique<brpc::Channel>();
            if (0 != brpc_channel->Init(rpc_path, opts))
            {
                LOG_ERROR(log, "Failed to initialize RPC channel for UDF");
                brpc_channel.reset(nullptr);
                return;
            }

            stub = std::make_unique<UDF_Stub>(brpc_channel.get());
            std::ostringstream os;
            brpc::DescribeOptions opt;
            brpc_channel->Describe(os, opt);
            LOG_INFO(log, "UDF " + os.str());
        }

        uint64_t createExecutable(const String & name, const ASTExternalFunction * ast, const ContextPtr & context)
        {
            std::string_view sv(name);

            auto pos = sv.find_last_of('.');
            auto db = sv.substr(0, pos == std::string_view::npos ? 0 : pos);
            auto fn = sv.substr(pos + 1);

            auto dir_path = db.empty() ? path : fmt::format("{}/{}", path, escapeForFileName(db));
            if (!std::filesystem::exists(dir_path))
                mkdir(dir_path.c_str(), 0755);

            auto file_path = fmt::format("{}/{}.py", dir_path, escapeForFileName(fn));

            if (base)
            {
                LOG_DEBUG(log, "Not the first UDF client on the machine, skip create UDF.");
                return std::filesystem::last_write_time(file_path).time_since_epoch().count();
            }

            if (std::filesystem::exists(file_path))
                remove(file_path.c_str());

            LOG_DEBUG(log, "Creating " + file_path);

            {
                WriteBufferFromOwnString create_statement_buf;
                std::stringstream stream;
                WriteBufferFromFile out(file_path, ast->body.size(), O_WRONLY | O_CREAT | O_EXCL);
                writeString(ast->body, out);
                out.next();

                if (context->getSettingsRef().fsync_metadata)
                    out.sync();
                out.close();
            }

            auto init_file_path = fmt::format("{}/__init__.py", dir_path);
            if (!std::filesystem::exists(init_file_path))
            {
                WriteBufferFromFile out(init_file_path, 0, O_WRONLY | O_CREAT | O_EXCL);
                out.next();
                out.close();
            }

            return std::filesystem::last_write_time(file_path).time_since_epoch().count();
        }

        void dropExecutable(const String & name)
        {
            if (base)
            {
                LOG_DEBUG(log, "Not the first UDF client on the machine, skip remove UDF.");
                return;
            }

            std::string_view sv(name);

            auto pos = sv.find_last_of('.');
            auto db = sv.substr(0, pos == std::string_view::npos ? 0 : pos);
            auto fn = sv.substr(pos + 1);

            auto dir_path = db.empty() ? path : fmt::format("{}/{}", path, escapeForFileName(db));
            auto file_path = fmt::format("{}/{}.py", dir_path, escapeForFileName(fn));
            LOG_DEBUG(log, "Removing " + file_path);

            if (std::filesystem::exists(file_path))
                remove(file_path.c_str());
        }
    };

    std::unique_ptr<UDFContext> udf_context;

    uint64_t getPrefix()
    {
        uint16_t id = udf_context->id.fetch_add(1, std::memory_order_relaxed);
        uint32_t offset = udf_context->base << 16;

        return offset + id;
    }

    void freeShm(struct metastoreex * pms, uint64_t prefix)
    {
        char filename[PATH_MAX];
        for (uint16_t idx = 0; idx < pms->idx; idx++)
        {
            shm_file_fmt(filename, prefix, idx);
            shm_unlink(filename);
        }
    }

    struct ScalarUDFArg
    {
        const DB::String & name;
        const ColumnsWithTypeAndName & arguments;
        ColumnWithTypeAndName & result;
        size_t row_cnt;
        uint64_t version;
    };

    struct AggregateUDFArg
    {
        struct ScalarUDFArg scalar;
        uint32_t method;
        //uint32_t batch_f_id;
        const ColumnWithTypeAndName & state_map;
        size_t state_cnt;
    };

    void serializeOutput(struct metastoreex & storeex, const struct ScalarUDFArg * arg)
    {
        uint64_t prefix = getPrefix();
        storeex.prefix = prefix;
        storeex.idx = 0;
        storeex.ms.isOut = true;

        /* serialization output*/
        serializeColumn(arg->result, &storeex);

        storeex.ms.isOut = false;
    }

    template <typename T>
    std::optional<std::string> RpcUDFCall(const T * req) noexcept
    {
        google::protobuf::Empty rsp;
        brpc::Controller cntl;

        if (!udf_context->stub)
            return "External UDF is not initialized";

        if constexpr (std::is_same<T, ScalarReq>::value)
            udf_context->stub->ScalarCall(&cntl, req, &rsp, nullptr);
        else if constexpr (std::is_same<T, AggregateReq>::value)
            udf_context->stub->AggregateCall(&cntl, req, &rsp, nullptr);

        if (cntl.Failed())
            return cntl.ErrorText();

        return std::nullopt;
    }

    const char * formatErrorMsg(std::string & s)
    {
        size_t pos = 0;

        while (true)
        {
            size_t curr = s.find_first_of('[', pos);
            if (curr == std::string::npos)
                break;
            pos = curr + 1;

            curr = s.find_first_of(']', pos);
            if (curr == std::string::npos)
                break;
            pos = curr + 1;
        }

        if ((s.back() == ')' && s[pos] == '(') || (s.back() == '"' && s[pos] == '"'))
        {
            pos++;
            s.pop_back();
        }

        return s.c_str() + pos;
    }

    template <typename T>
    void UDFClientCall(const T & arg)
    {
        const struct ScalarUDFArg * scalar;
        struct metastoreex storeex;

        if constexpr (std::is_same_v<T, AggregateUDFArg>)
        {
            scalar = &arg.scalar;
            serializeOutput(storeex, scalar);
            /* serialization state_map for UDAF*/
            if (arg.method & (ADD_BATCH | MERGE_DATA_BATCH))
                serializeColumn(arg.state_map, &storeex);
        }
        else
        {
            scalar = &arg;
            serializeOutput(storeex, scalar);
        }

        /* serialization input*/
        for (const auto & col : scalar->arguments)
            serializeColumn(col, &storeex);

        ScalarReq * req;

        if constexpr (std::is_same_v<T, AggregateUDFArg>)
        {
            req = new ScalarReq();
        }
        else
        {
#pragma clang diagnostic ignored "-Walloca"
            req = static_cast<ScalarReq *>(alloca(sizeof(ScalarReq)));
            new (req) ScalarReq;
        }

        /* prepare protobuf */
        size_t meta_bytes;
        meta_bytes = storeex.ms.metas.size() * sizeof(struct UDFMeta);
        req->set_f_name(scalar->name);
        req->set_cnt(storeex.ms.metas.size());
        req->set_metas(storeex.ms.metas.data(), meta_bytes);
        if (!storeex.ms.extras.empty())
            req->set_extras(storeex.ms.extras.data(), storeex.ms.extras.size());
        req->set_rows(scalar->row_cnt);
        req->set_version(scalar->version);
        req->set_prefix(storeex.prefix);

        std::optional<std::string> ret;
        if constexpr (std::is_same_v<T, AggregateUDFArg>)
        {
            AggregateReq aggregate_req;

            aggregate_req.set_method(arg.method);
            aggregate_req.set_unique_states_count(arg.state_cnt);
            aggregate_req.set_allocated_scalar_params(req);
            ret = RpcUDFCall<AggregateReq>(&aggregate_req);
        }
        else
        {
            ret = RpcUDFCall<ScalarReq>(req);
        }

        /* copy back data from output shared memory */
        for (const auto & cb : storeex.ms.output)
            cb(!ret.has_value());

        /* free all shared memory files */
        freeShm(&storeex, storeex.prefix);

        if (ret.has_value())
            throw Exception(formatErrorMsg(*ret), ErrorCodes::EXTERNAL_LIBRARY_ERROR);
    }

    void initContext(const Poco::Util::AbstractConfiguration * cfg, Poco::Logger * log)
    {
        brpc::ChannelOptions options;

        udf_context = std::make_unique<UDFContext>();

        udf_context->log = log;
        udf_context->path = getUDFPath(cfg);
        udf_context->uds_path = getUDSPath(udf_context->path);
        udf_context->base = cfg->getUInt("udf_base", 0);
        options.timeout_ms = cfg->getUInt("udf_manager_server.timeout_ms", 20000);
        options.max_retry = cfg->getUInt("udf_manager_server.max_retry", 1);
        if (udf_context->uds_path.size() >= sizeof(sockaddr_un::sun_path))
        {
            LOG_INFO(
                log, "UDF initializaiton failed due to socket path " + udf_context->uds_path + " exceeds maximum length of 108 bytes.");
            return;
        }
        udf_context->createClient(&options);
    }

    class UserDefinedFunction final : public IFunction
    {
    public:
        explicit UserDefinedFunction(const String & name_, ASTExternalFunction::Arguments && args_) : name(name_), args(args_) { }

        String getName() const override { return name; }

        bool isVariadic() const override { return true; }

        size_t getNumberOfArguments() const override { return 0; }

        bool isStateful() const override { return args.flags & BIT(UDFFlag::Stateful); }

        bool isSuitableForConstantFolding() const override { return args.flags & BIT(UDFFlag::SuitableForConstantFolding); }

        bool isInjective(const ColumnsWithTypeAndName & /*sample_columns*/) const override { return args.flags & BIT(UDFFlag::Injective); }

        bool isDeterministic() const override { return args.flags & BIT(UDFFlag::Deterministic); }

        bool isDeterministicInScopeOfQuery() const override { return args.flags & BIT(UDFFlag::DeterministicInScopeOfQuery); }

        bool hasInformationAboutMonotonicity() const override
        {
            uint32_t monotonicity = BIT(UDFFlag::AlwaysPositiveMonotonicity) | BIT(UDFFlag::AlwaysNegativeMonotonicity);

            return args.flags & monotonicity;
        }

        bool canBeExecutedOnDefaultArguments() const override { return args.flags & BIT(UDFFlag::CanBeExecutedOnDefaultArguments); }


        Monotonicity getMonotonicityForRange(const IDataType & /*type*/, const Field & /*left*/, const Field & /*right*/) const override
        {
            if (hasInformationAboutMonotonicity())
                return Monotonicity(true, args.flags & BIT(UDFFlag::AlwaysPositiveMonotonicity), true);
            else
                return Monotonicity(false, false, false);
        }

        bool useDefaultImplementationForConstants() const override
        {
            return args.flags & BIT(UDFFlag::UseDefaultImplementationForConstants);
        }

        bool useDefaultImplementationForNulls() const override
        {
            bool res = args.flags & BIT(UDFFlag::UseDefaultImplementationForNulls);
            return res;
        }

        bool useDefaultImplementationForLowCardinalityColumns() const override
        {
            return args.flags & BIT(UDFFlag::UseDefaultImplementationForLowCardinalityColumns);
        }

        bool canBeExecutedOnLowCardinalityDictionary() const override
        {
            return args.flags & BIT(UDFFlag::CanBeExecutedOnLowCardinalityDictionary);
        }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (args.ret)
                return args.ret;

            return arguments[0];
        }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            /// Do not start user defined script during query analysis. Because user script startup could be heavy.
            if (input_rows_count == 0)
                return result_type->createColumn();

            ColumnWithTypeAndName result(result_type->createColumn()->cloneResized(input_rows_count), result_type, "");

            const struct ScalarUDFArg arg = {
                .name = name,
                .arguments = arguments,
                .result = result,
                .row_cnt = input_rows_count,
                .version = args.version,
            };

            UDFClientCall<ScalarUDFArg>(arg);
            return result.column;
        }

    private:
        String name;
        ASTExternalFunction::Arguments args;
    };

    class UserDefinedAggregateFunction : public IAggregateFunction
    {
    public:
        UserDefinedAggregateFunction(
            const std::string & name_,
            const DataTypes & argument_types_,
            const Array & parameters_,
            const ASTExternalFunction::Arguments & args_)
            : IAggregateFunction(argument_types_, parameters_), args(args_), state_type(args.ret->getTypeId()), name(name_)
        {
        }

        String getName() const override { return name; }

        DataTypePtr getReturnType() const override { return args.ret; }

        void create(AggregateDataPtr place) const override
        {
            switch (state_type)
            {
#define V(IDX, TYPE) \
    case TypeIndex::IDX: \
        new (place) TYPE(); \
        break;
                APPLY_FOR_EACH_VECTOR_TYPE(V)
#undef V
                default:
                    throw Exception("UDAF type not supported.", ErrorCodes::UNSUPPORTED_METHOD);
            }
        }

        void destroy(AggregateDataPtr place) const noexcept override
        {
            switch (state_type)
            {
#define V(IDX, TYPE) \
    case TypeIndex::IDX: \
        (*reinterpret_cast<TYPE *>(place)).~TYPE(); \
        break;
                APPLY_FOR_EACH_VECTOR_TYPE(V)
#undef V
                default:
                    break;
            }
        }

        bool hasTrivialDestructor() const override { return true; }

        size_t sizeOfData() const override
        {
            switch (state_type)
            {
#define V(IDX, TYPE) \
    case TypeIndex::IDX: \
        return sizeof(TYPE);
                APPLY_FOR_EACH_VECTOR_TYPE(V)
#undef V
                default:
                    throw Exception("UDAF type not supported.", ErrorCodes::UNSUPPORTED_METHOD);
            }
        }

        /// NOTE: Currently not used (structures with aggregation state are put without alignment).
        size_t alignOfData() const override
        {
            switch (state_type)
            {
#define V(IDX, TYPE) \
    case TypeIndex::IDX: \
        return alignof(TYPE);
                APPLY_FOR_EACH_VECTOR_TYPE(V)
#undef V
                default:
                    throw Exception("UDAF type not supported.", ErrorCodes::UNSUPPORTED_METHOD);
            }
        }

        /* UDAF only use batch functions */
        void add(AggregateDataPtr, const IColumn **, size_t, Arena *) const override { }
        void merge(AggregateDataPtr, ConstAggregateDataPtr, Arena *) const override { }

        void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
        {
            switch (state_type)
            {
#define V(IDX, TYPE) \
    case TypeIndex::IDX: \
        writeBinary(*reinterpret_cast<const TYPE *>(place), buf); \
        break;
                APPLY_FOR_EACH_VECTOR_TYPE(V)
#undef V
                default:
                    throw Exception("UDAF type not supported.", ErrorCodes::UNSUPPORTED_METHOD);
            }
        }

        void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
        {
            switch (state_type)
            {
#define V(IDX, TYPE) \
    case TypeIndex::IDX: { \
        readBinary(*reinterpret_cast<TYPE *>(place), buf); \
        break; \
    }
                APPLY_FOR_EACH_VECTOR_TYPE(V)
#undef V
                default:
                    throw Exception("UDAF type not supported.", ErrorCodes::UNSUPPORTED_METHOD);
            }
        }

        bool allocatesMemoryInArena() const override { return false; }

        void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
        {
            switch (state_type)
            {
#define V(IDX, TYPE) \
    case TypeIndex::IDX: { \
        auto val = *reinterpret_cast<const TYPE *>(place); \
        static_cast<ColumnVector<DB::TYPE> &>(to).getData().push_back(val); \
        break; \
    }
                APPLY_FOR_EACH_VECTOR_TYPE(V)
#undef V
                default:
                    throw Exception("UDAF type not supported.", ErrorCodes::UNSUPPORTED_METHOD);
            }
        }

        AddFunc getAddressOfAddFunction() const override { return nullptr; }

        void addBatch(
            size_t batch_size, AggregateDataPtr * places, size_t place_offset, const IColumn ** columns, Arena *, ssize_t) const override
        {
            // convert non-linear places to linear states
            std::unordered_map<char *, UInt64> states;
            // each row point to index of states
            ColumnWithTypeAndName row2state = createStateMap(batch_size, states, places);
            size_t state_cnt = states.size();
            ColumnWithTypeAndName result(args.ret->createColumn()->cloneResized(state_cnt), args.ret, "");

            ColumnsWithTypeAndName inputs = createInputs(columns);

            const struct AggregateUDFArg arg = {
            .scalar = {
                .name = name,
                .arguments = inputs,
                .result = result,
                .row_cnt = batch_size, // LY-TODO: swap row_cnt and state_cnt
            },
            .method = Method::ADD_BATCH,
            .state_map = row2state,
            .state_cnt = state_cnt,
        };

            UDFClientCall<AggregateUDFArg>(arg);

            for (const auto & [place, row] : states)
                writeResultBack(place + place_offset, result, row);
        }

        void addBatchArray(
            size_t /*batch_size*/,
            AggregateDataPtr * /*places*/,
            size_t /*place_offset*/,
            const IColumn ** /*columns*/,
            const UInt64 * /*offsets*/,
            Arena * /*arena*/) const override
        {
            throw Exception("Array combinator is not supported by UDAF", ErrorCodes::UNSUPPORTED_METHOD);
        }

        void addBatchSinglePlaceNotNull(
            size_t /*batch_size*/,
            AggregateDataPtr /*place*/,
            const IColumn ** /*columns*/,
            const UInt8 * /*null_map*/,
            Arena * /*arena*/,
            ssize_t /*if_argument_pos = -1*/) const override
        {
            throw Exception("Null combinator is not supported by UDAF", ErrorCodes::UNSUPPORTED_METHOD);
        }

        void addBatchSinglePlaceFromInterval(
            size_t /*batch_begin*/,
            size_t /*batch_end*/,
            AggregateDataPtr /*place*/,
            const IColumn ** /*columns*/,
            Arena * /*arena*/,
            ssize_t /*if_argument_pos = -1*/) const override
        {
            throw Exception("Interval is not supported by UDAF", ErrorCodes::UNSUPPORTED_METHOD);
        }

        void addBatchLookupTable8(
            size_t rows,
            AggregateDataPtr * map,
            size_t place_offset,
            std::function<void(AggregateDataPtr &)> init,
            const UInt8 * key,
            const IColumn ** columns,
            Arena *) const override
        {
            std::bitset<256> bitmap;

            for (size_t row = 0; row < rows; row++)
            {
                UInt8 id = key[row];
                if (bitmap[id])
                    continue;
                bitmap[id] = true;
                if (bitmap.all())
                    break;
            }

            size_t state_cnt = bitmap.count();
            ColumnWithTypeAndName result(args.ret->createColumn()->cloneResized(state_cnt), args.ret, "");
            auto col = ColumnUInt8::create(rows);
            // TODO: avoid the copy
            col->insertData(reinterpret_cast<const char *>(key), rows);
            ColumnWithTypeAndName states(std::move(col), std::make_unique<DataTypeUInt8>(), "");

            ColumnsWithTypeAndName inputs = createInputs(columns);

            const struct AggregateUDFArg arg = {
            .scalar = {
                .name = name,
                .arguments = inputs,
                .result = result,
                .row_cnt = rows,
            },
            .method = Method::ADD_BATCH,
            .state_map = states,
            .state_cnt = state_cnt,
        };

            UDFClientCall<AggregateUDFArg>(arg);

            for (size_t i = 0; i < 256; i++)
            {
                if (!bitmap[i])
                    continue;
                AggregateDataPtr & place = map[i];
                if (!place)
                    init(place);

                writeResultBack(place + place_offset, result, i);
            }
        }

        void addBatchSinglePlace(size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena *, ssize_t) const override
        {
            auto state = ColumnWithTypeAndName(args.ret->createColumn()->cloneResized(1), args.ret, "");
            ColumnsWithTypeAndName inputs = createInputs(columns);

            const struct AggregateUDFArg arg = {
            .scalar = {
                .name = name,
                .arguments = inputs,
                .result = state,
                .row_cnt = batch_size,
            },
            .method = Method::ADD_BATCH_SINGLE_PLACE,
            .state_map = {},
            .state_cnt = 1,
        };

            UDFClientCall<AggregateUDFArg>(arg);

            writeResultBack(place, state, 0);
        }

        void destroyBatch(size_t size, AggregateDataPtr * places, size_t place_offset) const noexcept override
        {
            for (size_t i = 0; i < size; ++i)
                (this)->destroy(places[i] + place_offset);
        }


        void insertResultIntoBatch(
            size_t batch_size, AggregateDataPtr * places, size_t place_offset, IColumn & to, Arena * arena, bool destroy_place_after_insert)
            const override
        {
            size_t batch_index = 0;

            try
            {
                for (; batch_index < batch_size; ++batch_index)
                {
                    insertResultInto(places[batch_index] + place_offset, to, arena);

                    if (destroy_place_after_insert)
                        destroy(places[batch_index] + place_offset);
                }
            }
            catch (...)
            {
                for (size_t destroy_index = batch_index; destroy_index < batch_size; ++destroy_index)
                    destroy(places[destroy_index] + place_offset);

                throw;
            }
        }

        void
        mergeBatch(size_t batch_size, AggregateDataPtr * places, size_t place_offset, const AggregateDataPtr * rhs, Arena *) const override
        {
            ColumnsWithTypeAndName inputs;

            {
                auto lhs_state_s = args.ret->createColumn()->cloneEmpty();
                auto rhs_state_s = args.ret->createColumn()->cloneEmpty();

                for (size_t i = 0; i < batch_size; ++i)
                {
                    if (!places[i])
                        continue;

                    insertState(lhs_state_s, places[i], place_offset);
                    insertState(rhs_state_s, rhs[i], 0);
                }

                inputs.emplace_back(std::move(lhs_state_s), args.ret, "");
                inputs.emplace_back(std::move(rhs_state_s), args.ret, "");
            }

            // create output column
            size_t state_cnt = inputs[0].column->size();
            auto result = ColumnWithTypeAndName(args.ret->createColumn()->cloneResized(state_cnt), args.ret, "");

            const struct AggregateUDFArg arg = {
            .scalar = {
                .name = name,
                .arguments = inputs,
                .result = result,
                .row_cnt = batch_size,
            },
            .method = Method::MERGE_BATCH,
            .state_map = {},
            .state_cnt = state_cnt,
        };

            UDFClientCall<AggregateUDFArg>(arg);

            for (size_t i = 0, j = 0; i < batch_size; ++i)
            {
                if (!places[i])
                    continue;

                writeResultBack(places[i] + place_offset, result, j++);
            }
        }

        // LY-TODO: call mergeDataBatch / destroyBatch in Aggregator.cpp
#if 0
    void mergeDataBatch(size_t batch_size,
                        std::vector<AggregateDataPtr> & places,
                        size_t place_offset,
                        std::vector<AggregateDataPtr> & rhs,
                        Arena *) const
    {
        if (!batch_size || places.empty())
            return;

        auto state_map_s = DataTypeUInt64().createColumn()->cloneEmpty();
        std::unordered_map<char *, UInt64> state_to_row_map;
        ColumnsWithTypeAndName inputs;

        {
            auto lhs_state_s = args.ret->createColumn()->cloneEmpty();
            auto rhs_state_s = args.ret->createColumn()->cloneEmpty();

            for (size_t i = 0, idx = 0; i < batch_size; ++i)
            {
                const auto &[it, inserted] = state_to_row_map.emplace(places[i], idx);

                if (inserted)
                {
                    idx++;
                    insertState(lhs_state_s, places[i], place_offset);
                }
                state_map_s->insert(it->second);
                insertState(rhs_state_s, rhs[i], 0);
            }

            inputs.emplace_back(std::move(lhs_state_s), args.ret, "");
            inputs.emplace_back(std::move(rhs_state_s), args.ret, "");
        }
        auto state_map = ColumnWithTypeAndName(std::move(state_map_s),
                                               std::make_unique<DataTypeUInt64>(), "");

        size_t state_cnt = state_to_row_map.size();
        auto state = ColumnWithTypeAndName(args.ret->createColumn()->cloneResized(state_cnt), args.ret, "");

        const struct AggregateUDFArg arg = {
            .scalar = {
                .name = name,
                .arguments = inputs,
                .result = state,
                .row_cnt = batch_size,
            },
            .method = Method::MERGE_DATA_BATCH,
            .state_map = state_map,
            .state_cnt = state_cnt,
        };

        UDFClientCall<AggregateUDFArg>(arg);

        for (const auto & [place, row] : state_to_row_map)
            writeResultBack(place + place_offset, state, row);
    }
#endif

    private:
        void writeResultBack(AggregateDataPtr place, const ColumnWithTypeAndName & col_state, size_t i) const
        {
            switch (state_type)
            {
#define V(IDX, TYPE) \
    case TypeIndex::IDX: { \
        const char * ref = col_state.column->getDataAt(i).data; \
        *reinterpret_cast<TYPE *>(place) = *reinterpret_cast<const TYPE *>(ref); \
        break; \
    }
                APPLY_FOR_EACH_VECTOR_TYPE(V)
#undef V
                default:
                    throw Exception("UDAF type not supported.", ErrorCodes::UNSUPPORTED_METHOD);
            }
        }

        static ColumnWithTypeAndName
        createStateMap(size_t batch_size, std::unordered_map<char *, UInt64> & state_to_row_map, const AggregateDataPtr * places)
        {
            /* row_id -> state address id mapping */
            auto state_map = DataTypeUInt64().createColumn()->cloneEmpty();

            for (size_t i = 0, idx = 0; i < batch_size; ++i)
            {
                const auto & [it, inserted] = state_to_row_map.emplace(places[i], idx);

                if (inserted)
                    idx++;

                state_map->insert(it->second);
            }

            return {std::move(state_map), std::make_unique<DataTypeUInt64>(), ""};
        }

        ColumnsWithTypeAndName createInputs(const IColumn ** columns) const
        {
            auto const & arg_types = getArgumentTypes();
            ColumnsWithTypeAndName inputs;

            for (size_t i = 0; i < arg_types.size(); ++i)
                inputs.emplace_back(ColumnWithTypeAndName{columns[i]->getPtr(), arg_types[i], columns[i]->getName()});

            return inputs;
        }

        void insertState(MutableColumnPtr & state_s, ConstAggregateDataPtr place, size_t place_offset) const
        {
            if (!place)
                return;

            switch (state_type)
            {
#define V(IDX, TYPE) \
    case TypeIndex::IDX: \
        state_s->insert(*reinterpret_cast<const TYPE *>(place + place_offset)); \
        break;
                APPLY_FOR_EACH_VECTOR_TYPE(V)
#undef V
                default:
                    throw Exception("UDAF type not supported.", ErrorCodes::UNSUPPORTED_METHOD);
            }
        }

        ASTExternalFunction::Arguments args;
        DB::TypeIndex state_type;
        String name;
    };

}

UserDefinedExternalFunctionFactory::UserDefinedExternalFunctionFactory() : log{&Poco::Logger::get("UserDefinedExternalFunctionFactory")}
{
}

UserDefinedExternalFunctionFactory & UserDefinedExternalFunctionFactory::instance()
{
    static UserDefinedExternalFunctionFactory result;
    return result;
}

AggregateFunctionPtr
UserDefinedExternalFunctionFactory::tryGet(const String & function_name, const DataTypes & argument_types, const Array & parameters)
{
    ASTExternalFunction::Arguments args;

    {
        ReadLock lock(mutex);
        const auto it = functions[to_underlying(UDFFunctionType::Aggregate)].find(function_name);

        if (it == functions[to_underlying(UDFFunctionType::Aggregate)].end())
            return nullptr;

        args = it->second;
    }

    return std::make_shared<UserDefinedAggregateFunction>(function_name, argument_types, parameters, args);
}

FunctionOverloadResolverPtr UserDefinedExternalFunctionFactory::tryGet(const String & function_name)
{
    ASTExternalFunction::Arguments args;

    {
        ReadLock lock(mutex);
        const auto it = functions[to_underlying(UDFFunctionType::Scalar)].find(function_name);

        if (it == functions[to_underlying(UDFFunctionType::Scalar)].end())
            return nullptr;

        args = it->second;
    }

    auto function = std::make_shared<UserDefinedFunction>(function_name, std::move(args));
    return std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(function));
}

bool UserDefinedExternalFunctionFactory::tryGetVersion(const String & function_name, uint64_t ** version_ptr)
{
    ReadLock lock(mutex);
    const auto it = functions[to_underlying(UDFFunctionType::Scalar)].find(function_name);
    if (it != functions[to_underlying(UDFFunctionType::Scalar)].end())
    {
        *version_ptr = &it->second.version;
        return true;
    }

    return false;
}

bool UserDefinedExternalFunctionFactory::has(const String & function_name, UDFFunctionType type)
{
    ReadLock lock(mutex);
    if (type != UDFFunctionType::Count)
        return functions[to_underlying(type)].contains(function_name);

    for (const auto & fns : functions)
    {
        if (fns.contains(function_name))
            return true;
    }
    return false;
}

bool UserDefinedExternalFunctionFactory::setFunction(const String & function_name, const IAST & ast, const ContextPtr & context)
{
    const auto & create = assert_cast<const ASTCreateFunctionQuery &>(ast);
    const auto * core = assert_cast<const ASTExternalFunction *>(create.function_core.get());
    udf_context->createExecutable(function_name, core, context);
    auto type = getFunctionType(core->args.flags);

    if (type == UDFFunctionType::Aggregate)
        throw Exception("UDAF not supported yet.", ErrorCodes::UNSUPPORTED_METHOD);

    WriteLock lock(mutex);
    auto [it, inserted] = functions[to_underlying(type)].emplace(function_name, core->args);

    it->second.version = create.version;
    return inserted;
}

bool UserDefinedExternalFunctionFactory::removeFunction(const String & function_name)
{
    udf_context->dropExecutable(function_name);

    WriteLock lock(mutex);
    for (auto & fns : functions)
    {
        if (1 == fns.erase(function_name))
            return true;
    }
    return false;
}

void UserDefinedExternalFunctionFactory::setAllFunctions(std::vector<std::pair<String, ASTPtr>> & asts, const ContextPtr & context)
{
    std::unordered_map<String, ASTExternalFunction::Arguments> all[to_underlying(UDFFunctionType::Count)];

    if (!udf_context)
        initContext(&context->getConfigRef(), log);

    for (auto & [name, ast] : asts)
    {
        ASTPtr create = assert_cast<const ASTCreateFunctionQuery *>(ast.get())->function_core;
        const auto * core = assert_cast<const ASTExternalFunction *>(create.get());

        auto version = udf_context->createExecutable(name, core, context);
        auto type = getFunctionType(core->args.flags);
        LOG_TRACE(log, std::string("Added ") + getFunctionTypeStr(type) + " " + name);
        auto [it, _] = all[to_underlying(type)].emplace(std::move(name), core->args);
        it->second.version = version;
    }

    WriteLock lock(mutex);
    for (size_t i = 0; i < to_underlying(UDFFunctionType::Count); i++)
        functions[i] = std::move(all[i]);
}

}
