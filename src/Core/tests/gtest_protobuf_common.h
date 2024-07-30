#include <memory>
#include <random>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/QueryRewriter.h>
#include <Columns/Collator.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/DistributedStages/PlanSegmentSplitter.h>
#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Interpreters/InterpreterShowStatsQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/PlanOptimizer.h>
#include <Optimizer/Property/Property.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Protos/ReadWriteProtobuf.h>
#include <Protos/plan_node_utils.pb.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanPrinter.h>
#include <QueryPlan/PlanSerDerHelper.h>
#include <QueryPlan/QueryPlanner.h>
#include <Statistics/CacheManager.h>
#include <Statistics/CatalogAdaptor.h>
#include <fmt/format.h>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>
#include <Poco/NumberParser.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegment.h>

namespace DB::UnitTest
{

class ProtobufTest : public testing::Test
{
public:
    ProtobufTest() { }

    static void init_database()
    {
        tryRegisterFunctions();
        tryRegisterFormats();
        tryRegisterStorages();
        tryRegisterAggregateFunctions();
        tryRegisterHints();

        session_context = Context::createCopy(getContext().context);

        auto database_name = "db";

        if (DatabaseCatalog::instance().tryGetDatabase(database_name, session_context))
            DatabaseCatalog::instance().detachDatabase(session_context, database_name, true, false);

        auto database = std::make_shared<DatabaseMemory>(database_name, session_context);
        DatabaseCatalog::instance().attachDatabase(database_name, database);
        session_context->setCurrentDatabase(database_name);

        auto query_context = Context::createCopy(session_context);
        query_context->setSessionContext(session_context);
        query_context->setQueryContext(query_context);
        query_context->setCurrentQueryId("test_protobuf");
        query_context->createPlanNodeIdAllocator();
        query_context->createSymbolAllocator();
        query_context->createOptimizerMetrics();
        context = query_context;
    }

    static ASTPtr parse(const std::string & query, ContextMutablePtr query_context)
    {
        const char * begin = query.data();
        const char * end = begin + query.size();

        ParserQuery parser(end, ParserSettings::ANSI);
        auto ast = parseQuery(
            parser, begin, end, "", query_context->getSettingsRef().max_query_size, query_context->getSettingsRef().max_parser_depth);
        return ast;
    }

    static void init_tables()
    {
        std::vector<String> ddls = {
            "create table tb9(a UInt8, b Float64) Engine=CnchMergeTree() order by a",
            "create table tb10(a UInt16, b Float32) Engine=CnchMergeTree() order by a",
            "create table tb11(a UInt32, b Float32) Engine=CnchMergeTree() order by a",
        };

        for (auto & ddl : ddls)
        {
            ASTPtr ast = parse(ddl, session_context);
            if (auto * create = ast->as<ASTCreateQuery>())
            {
                auto engine = std::make_shared<ASTFunction>();
                engine->name = "Memory";
                auto storage = std::make_shared<ASTStorage>();
                storage->set(storage->engine, engine);
                create->set(create->storage, storage);
            }

            ThreadStatus thread_status;
            thread_status.attachQueryContext(session_context);
            InterpreterCreateQuery create_interpreter(ast, session_context);
            create_interpreter.execute();
        }
    }

    ~ProtobufTest() = default;

    static void SetUpTestCase()
    {
        init_database();
        init_tables();

        auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());

        test_data_types.emplace_back(std::make_shared<DataTypeString>());
        test_data_types.emplace_back(std::make_shared<DataTypeUInt32>());
        test_data_types.emplace_back(map_type);


        test_name_and_type_pairs.emplace_back("c1", std::make_shared<DataTypeString>());
        test_name_and_type_pairs.emplace_back("c2", std::make_shared<DataTypeUInt32>());
        test_name_and_type_pairs.emplace_back("maps", "subcol", map_type, std::make_shared<DataTypeString>());

        UUID uuid9 = UUID{UInt128{0, 0}};
        StorageID storage_id9 = {"db", "tb9", uuid9};
        UUID uuid10 = UUID{UInt128{0, 0}};
        StorageID storage_id10 = {"db", "tb10", uuid10};
        UUID uuid11 = UUID{UInt128{0, 0}};
        StorageID storage_id11 = {"db", "tb11", uuid11};
        test_storage_ids.emplace_back(storage_id9);
        test_storage_ids.emplace_back(storage_id10);
        test_storage_ids.emplace_back(storage_id11);

        // TODO: create these in table
    }

    static void TearDownTestCase()
    {
        test_storage_ids.clear();
        test_name_and_type_pairs.clear();
        test_data_types.clear();
        context = nullptr;
        session_context = nullptr;
    }

    using Message = google::protobuf::Message;

    static void compareProto(const Message & pb, const Message & pb2)
    {
        std::string str3;
        {
            google::protobuf::io::StringOutputStream stream(&str3);
            google::protobuf::io::CodedOutputStream output(&stream);
            output.SetSerializationDeterministic(true);
            pb.SerializeToCodedStream(&output);
        }
        auto hash3 = sipHash64(str3);

        auto hash1 = sipHash64Protobuf(pb);
        auto hash2 = sipHash64Protobuf(pb2);
        ASSERT_EQ(hash1, hash2);
        ASSERT_EQ(hash1, hash3);

        // use official compare method, so unordered map is correctedly handled
        if (google::protobuf::util::MessageDifferencer::Equals(pb, pb2))
        {
            return;
        }

        std::cout << "not equal" << std::endl;
        std::string dbg_str;
        std::string dbg_str2;
        google::protobuf::TextFormat::PrintToString(pb, &dbg_str);
        google::protobuf::TextFormat::PrintToString(pb2, &dbg_str2);
        ASSERT_EQ(dbg_str, dbg_str2);
    }

    static void compareStep(QueryPlanStepPtr a, QueryPlanStepPtr b)
    {
        auto is_equal = isPlanStepEqual(*a, *b);
        ASSERT_TRUE(is_equal);
        auto ha = hashPlanStep(*a);
        auto hb = hashPlanStep(*b);
        ASSERT_EQ(ha, hb);
    }

    static NameAndTypePair generateNameAndTypePair(std::default_random_engine & eng) { return test_name_and_type_pairs[eng() % 3]; }

    static std::shared_ptr<TableWriteStep::InsertTarget> generateTableWriteStepInsertTarget(std::default_random_engine & eng)
    {
        auto storage_id = test_storage_ids[eng() % 3];
        auto storage = DatabaseCatalog::instance().getTable(storage_id, context);
        NamesAndTypes columns;
        for (int i = 0; i < 2; ++i)
            columns.emplace_back(generateNameAndTypePair(eng));
        auto step = std::make_shared<TableWriteStep::InsertTarget>(storage, storage_id, columns, nullptr);
        return step;
    }

    // static  InsertTarget generateInsertTarget(std::default_random_engine & eng)
    // {
    //     InsertTarget res;
    //     res.storage_id = test_storage_ids[eng() % 3];
    //     // generate NamesAndTypes
    //     for (int i = 0; i < 2; ++i)
    //         res.columns.emplace_back(generateNameAndTypePair(eng));
    //     return res;
    // }

    static Field generateField(std::default_random_engine & eng)
    {
        static std::vector<Field> fields = {Field(-1), Field(42UL), Field(1.0), Field("hello world")};
        auto index = eng() % fields.size();
        return fields[index];
    }

    static FillColumnDescription generateFillColumnDescription(std::default_random_engine & eng)
    {
        FillColumnDescription res;
        res.fill_from = generateField(eng);
        res.fill_to = generateField(eng);
        res.fill_step = generateField(eng);
        return res;
    }

    static SortColumnDescription generateSortColumnDescription(std::default_random_engine & eng)
    {
        SortColumnDescription res;
        res.column_name = fmt::format("text{}", eng() % 100);
        res.column_number = eng() % 1000;
        res.direction = eng() % 1000 * -1;
        res.nulls_direction = eng() % 1000 * -1;
        if (eng() % 2 == 1)
            res.collator = std::make_shared<Collator>("zh");
        res.with_fill = eng() % 2 == 1;
        res.fill_description = generateFillColumnDescription(eng);
        return res;
    }

    static Block generateBlock(std::default_random_engine & eng, bool arr = false)
    {
        std::vector<std::string> columns = {"a", "b", "c", "col_0", "col_1", "col_3"};
        size_t rows = 10;
        size_t stride = 1;
        size_t start = 0;
        ColumnsWithTypeAndName cols;
        size_t size_of_row_in_bytes = columns.size() * sizeof(UInt64);
        for (size_t i = 0; i * sizeof(UInt64) < size_of_row_in_bytes; i++)
        {
            auto column = ColumnUInt64::create(rows, 0);
            for (size_t j = 0; j < rows; ++j)
            {
                column->getElement(j) = start + eng();
                start += stride;
            }
            cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), columns[i]);
        }
        if (arr)
        {
            auto val = ColumnUInt64::create(45, 0);
            auto offset = ColumnUInt64::create(10, 0);

            size_t all = 0;
            for (size_t i = 0; i < rows; ++i)
            {
                offset->getElement(i) = all + i;
                for (auto j = 0; j < i; ++j)
                {
                    val->getElement(all + j) = i;
                }
                all += i;
            }

            cols.emplace_back(
                ColumnArray::create(std::move(val), std::move(offset)),
                std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()),
                "arr");
        }

        return Block(cols);
    }

    static DataStream generateDataStream(std::default_random_engine & eng, bool arr = false)
    {
        DataStream res;
        res.header = generateBlock(eng, arr);
        for (int i = 0; i < 10; ++i)
            res.distinct_columns.emplace(fmt::format("text{}", eng() % 100));
        res.has_single_port = eng() % 2 == 1;
        for (int i = 0; i < 2; ++i)
            res.sort_description.emplace_back(generateSortColumnDescription(eng));
        res.sort_mode = static_cast<DataStream::SortMode>(eng() % 3);
        return res;
    }

    static SizeLimits generateSizeLimits(std::default_random_engine & eng)
    {
        SizeLimits res;
        res.max_rows = eng() % 1000;
        res.max_bytes = eng() % 1000;
        res.overflow_mode = static_cast<OverflowMode>(eng() % 3);
        return res;
    }

    static Partitioning generatePartitioning(std::default_random_engine & eng)
    {
        auto handle = static_cast<Partitioning::Handle>(eng() % 3);
        Names columns;
        for (int i = 0; i < 10; ++i)
            columns.emplace_back(fmt::format("text{}", eng() % 100));
        auto require_handle = eng() % 2 == 1;
        auto buckets = eng() % 1000;
        auto enforce_round_robin = eng() % 2 == 1;
        auto component = static_cast<Partitioning::Component>(eng() % 3);
        auto result = Partitioning(handle, columns, require_handle, buckets, nullptr, enforce_round_robin, component);
        return result;
    }

    static auto generateSetOperationStep(std::default_random_engine & eng)
    {
        DataStreams base_input_streams;
        for (int i = 0; i < 2; ++i)
            base_input_streams.emplace_back(generateDataStream(eng));
        auto base_output_stream = generateDataStream(eng);
        std::unordered_map<String, std::vector<String>> output_to_inputs;
        auto left_symbols = base_input_streams[0].header.getNames();
        auto right_symbols = base_input_streams[1].header.getNames();
        auto n = left_symbols.size();
        assert(n == right_symbols.size());

        for (int i = 0; i < n; ++i)
            output_to_inputs[fmt::format("text{}", eng() % 10000)] = std::vector{left_symbols[i], right_symbols[i]};
        return std::make_tuple(base_input_streams, base_output_stream, output_to_inputs);
    }

    static ASTPtr generateAST(std::default_random_engine & eng) { return std::make_shared<ASTLiteral>(eng() % 3); }


    static auto generateAssignments(std::default_random_engine & eng)
    {
        Names columns;
        columns.emplace_back(fmt::format("text{}", eng() % 100));
        columns.emplace_back(fmt::format("text{}", eng() % 100));
        columns.emplace_back(fmt::format("text{}", eng() % 100));

        Assignments assignments;
        NameToType name_to_type;
        for (auto col : columns)
        {
            assignments.emplace_back(col, std::make_shared<ASTIdentifier>(col));
            name_to_type[col] = std::make_shared<DataTypeString>();
        }
        return assignments;
    }

    static GroupingSetsParams generateGroupingSetsParams(std::default_random_engine & eng)
    {
        GroupingSetsParams res;
        // generate Names
        for (int i = 0; i < 10; ++i)
            res.used_key_names.emplace_back(fmt::format("text{}", eng() % 100));
        // generate ColumnNumbers
        for (int i = 0; i < 2; ++i)
            res.used_keys.emplace_back(eng() % 3);
        // generate ColumnNumbers
        for (int i = 0; i < 2; ++i)
            res.missing_keys.emplace_back(eng() % 3);
        return res;
    }

    static GroupingDescription generateGroupingDescription(std::default_random_engine & eng)
    {
        GroupingDescription res;
        // generate Names
        for (int i = 0; i < 10; ++i)
            res.argument_names.emplace_back(fmt::format("text{}", eng() % 100));
        res.output_name = fmt::format("text{}", eng() % 100);
        return res;
    }

    static AggregateDescription generateAggregateDescription(std::default_random_engine & /*eng*/, int i)
    {
        AggregateDescription res;
        AggregateFunctionProperties properties;
        res.function = AggregateFunctionFactory::instance().get("count", {}, {}, properties);
        res.parameters = {};
        // generate ColumnNumbers
        // for (int i = 0; i < 2; ++i)
        // res.arguments.emplace_back(eng() % 3);
        // generate Names
        // for (int i = 0; i < 10; ++i)
        // res.argument_names.emplace_back(fmt::format("text{}", eng() % 100));
        res.column_name = "col_" + std::to_string(i);
        res.mask_column = res.column_name;
        return res;
    }

    static Aggregator::Params generateAggregatorParams(std::default_random_engine & eng)
    {
        auto src_header = generateBlock(eng);
        auto intermediate_header = generateBlock(eng);
        ColumnNumbers keys;
        for (int i = 0; i < 2; ++i)
            keys.emplace_back(eng() % 3);
        AggregateDescriptions aggregates;
        for (int i = 0; i < 2; ++i)
            aggregates.emplace_back(generateAggregateDescription(eng, i));
        auto overflow_row = eng() % 2 == 1;
        auto max_rows_to_group_by = eng() % 1000;
        auto group_by_overflow_mode = static_cast<OverflowMode>(eng() % 3);
        auto group_by_two_level_threshold = eng() % 1000;
        auto group_by_two_level_threshold_bytes = eng() % 1000;
        auto max_bytes_before_external_group_by = eng() % 1000;
        auto enable_adaptive_spill = eng() % 2;
        auto spill_buffer_bytes_before_external_group_by = eng() % 1000;
        auto empty_result_for_aggregation_by_empty_set = eng() % 2 == 1;
        auto tmp_volume = context ? context->getTemporaryVolume() : nullptr;
        auto max_threads = eng() % 1000;
        auto min_free_disk_space = eng() % 1000;
        auto compile_aggregate_expressions = eng() % 2 == 1;
        auto min_count_to_compile_aggregate_expression = eng() % 1000;
        auto enable_lc_group_by_opt = eng() % 2 == 1;
        auto step = Aggregator::Params(
            src_header,
            keys,
            aggregates,
            overflow_row,
            max_rows_to_group_by,
            group_by_overflow_mode,
            group_by_two_level_threshold,
            group_by_two_level_threshold_bytes,
            max_bytes_before_external_group_by,
            enable_adaptive_spill,
            spill_buffer_bytes_before_external_group_by,
            empty_result_for_aggregation_by_empty_set,
            tmp_volume,
            max_threads,
            min_free_disk_space,
            compile_aggregate_expressions,
            min_count_to_compile_aggregate_expression,
            intermediate_header,
            enable_lc_group_by_opt);

        return step;
    }

    static AggregatingTransformParamsPtr generateAggregatingTransformParams(std::default_random_engine & eng)
    {
        auto params = generateAggregatorParams(eng);
        auto final = eng() % 2 == 1;
        auto step = std::make_shared<AggregatingTransformParams>(params, final);

        return step;
    }

    static InputOrderInfoPtr generateInputOrderInfo(std::default_random_engine & eng)
    {
        SortDescription order_key_prefix_descr;
        for (int i = 0; i < 2; ++i)
            order_key_prefix_descr.emplace_back(generateSortColumnDescription(eng));
        auto direction = eng() % 1000 * -1;
        auto res = std::make_shared<InputOrderInfo>(order_key_prefix_descr, direction);

        return res;
    }

    static ArrayJoinActionPtr generateArrayJoinAction(std::default_random_engine & eng)
    {
        (void)eng;
        NameSet columns;
        columns.insert("arr");
        auto is_left = eng() % 2 == 1;
        auto step = std::make_shared<ArrayJoinAction>(columns, is_left, context);

        return step;
    }

    static SelectQueryInfo generateSelectQueryInfo(std::default_random_engine &)
    {
        SelectQueryInfo res;
        auto list = std::make_shared<ASTExpressionList>();
        list->children.emplace_back(std::make_shared<ASTIdentifier>("a"));
        list->children.emplace_back(std::make_shared<ASTIdentifier>("b"));

        const auto generated_query = std::make_shared<ASTSelectQuery>();
        generated_query->setExpression(ASTSelectQuery::Expression::SELECT, list);
        const auto select_expression_list = generated_query->select();

        res.query = generated_query;
        res.view_query = nullptr;
        return res;
    }

    static AddressInfo generateAddressInfo(std::default_random_engine & eng)
    {
        auto host_name = fmt::format("localhost");
        auto port = eng() % 1000 + 10000;
        auto user = fmt::format("text{}", eng() % 100);
        auto password = fmt::format("text{}", eng() % 100);
        return AddressInfo(host_name, port, user, password);
    }

    static std::shared_ptr<PlanSegmentInput> generatePlanSegmentInput(std::default_random_engine &)
    {
        Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};
        AddressInfo local_address("localhost", 0, "test", "123456");

        auto input = std::make_shared<PlanSegmentInput>(header, PlanSegmentType::EXCHANGE);
        input->setExchangeParallelSize(2);
        input->setExchangeId(3);
        input->setPlanSegmentId(4);
        input->insertSourceAddress(local_address);
        return input;
    }

    static std::shared_ptr<PlanSegmentOutput> generatePlanSegmentOutput(std::default_random_engine & eng)
    {
        Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};
        auto output = std::make_shared<PlanSegmentOutput>(header, PlanSegmentType::EXCHANGE);
        output->setExchangeParallelSize(2);
        output->setExchangeId(3);
        output->setPlanSegmentId(4);
        output->setKeepOrder(true);
        output->setShuffleFunctionName("bucket");
        Array params;
        params.emplace_back(generateField(eng));
        params.emplace_back(generateField(eng));
        output->setShuffleFunctionParams(params);
        return output;
    }

    static WindowFrame generateWindowFrame(std::default_random_engine & eng)
    {
        WindowFrame res;
        res.is_default = eng() % 2 == 1;
        res.type = static_cast<WindowFrame::FrameType>(eng() % 3);
        res.begin_type = static_cast<WindowFrame::BoundaryType>(eng() % 3);
        res.begin_offset = generateField(eng);
        res.begin_preceding = eng() % 2 == 1;
        res.end_type = static_cast<WindowFrame::BoundaryType>(eng() % 3);
        res.end_offset = generateField(eng);
        res.end_preceding = eng() % 2 == 1;
        return res;
    }

    static WindowFunctionDescription generateWindowFunctionDescription(std::default_random_engine & eng)
    {
        WindowFunctionDescription res;
        res.column_name = fmt::format("text{}", eng() % 100);

        // generate DataTypes
        for (int i = 0; i < 2; ++i)
            res.argument_types.emplace_back(test_data_types[eng() % 2]);

        res.function_parameters = Array{};
        AggregateFunctionProperties properties;
        res.aggregate_function = AggregateFunctionFactory::instance().get("uniq", res.argument_types, res.function_parameters, properties);

        // generate Names
        for (int i = 0; i < 2; ++i)
            res.argument_names.emplace_back(fmt::format("text{}", eng() % 100));
        return res;
    }

    static WindowDescription generateWindowDescription(std::default_random_engine & eng)
    {
        WindowDescription res;
        res.window_name = fmt::format("text{}", eng() % 100);
        // generate SortDescription
        for (int i = 0; i < 2; ++i)
            res.partition_by.emplace_back(generateSortColumnDescription(eng));
        // generate SortDescription
        for (int i = 0; i < 2; ++i)
            res.order_by.emplace_back(generateSortColumnDescription(eng));
        // generate SortDescription
        for (int i = 0; i < 2; ++i)
            res.full_sort_description.emplace_back(generateSortColumnDescription(eng));
        res.frame = generateWindowFrame(eng);
        // generate std::vector<WindowFunctionDescription>
        for (int i = 0; i < 2; ++i)
            res.window_functions.emplace_back(generateWindowFunctionDescription(eng));
        return res;
    }

    static RuntimeFilterBuildInfos generateRuntimeFilterBuildInfos(std::default_random_engine & eng)
    {
        auto id = eng() % 1000;
        auto distribution = static_cast<RuntimeFilterDistribution>(eng() % 3);
        return RuntimeFilterBuildInfos(id, distribution);
    }

public:
    static ContextMutablePtr session_context;
    static ContextMutablePtr context;
    static DataTypes test_data_types;
    static std::vector<NameAndTypePair> test_name_and_type_pairs;
    static std::vector<StorageID> test_storage_ids;
};
}
