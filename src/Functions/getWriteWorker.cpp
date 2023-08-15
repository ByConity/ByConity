#include <Functions/IFunction.h>

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include "Interpreters/DatabaseCatalog.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}


template <class Name, bool ReturnArray>
class FunctionGetWriteWorkerBase : public IFunction
{
public:
    explicit FunctionGetWriteWorkerBase(ContextPtr c) : context(c) { }
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionGetWriteWorkerBase>(context); }

    static constexpr auto name = Name::name;
    String getName() const override { return Name::name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(
                "Function " + getName() + " requires 2 or 3 parameters: database, table, http/tcp Passed " + toString(arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto & arg : arguments)
        {
            const IDataType * argument_type = arg.type.get();
            const DataTypeString * argument = checkAndGetDataType<DataTypeString>(argument_type);
            if (!argument)
                throw Exception(
                    "Illegal column " + arg.name + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

        if constexpr (ReturnArray)
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        else
            return std::make_shared<DataTypeString>();
    }

    static String getStringFromConstColumn(const ColumnPtr & column)
    {
        if (!isColumnConst(*column))
            throw Exception("The argument of function " + toString(name) + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);
        return static_cast<const ColumnConst &>(*column).getField().get<String>();
    }

    enum Protocol
    {
        TCP,
        HTTP,
    };

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        String database = getStringFromConstColumn(arguments[0].column);
        String table = getStringFromConstColumn(arguments[1].column);

        Protocol protocol = TCP;
        if (arguments.size() == 3)
        {
            auto s_protocol = Poco::toLower(getStringFromConstColumn(arguments[2].column));
            if (s_protocol == "tcp")
                protocol = TCP;
            else if (s_protocol == "http")
                protocol = HTTP;
            else
                throw Exception("Expected tcp or http in 3rd argument of function " + getName(), ErrorCodes::BAD_ARGUMENTS);
        }

        auto storage = DatabaseCatalog::instance().getTable({database, table}, context);
        auto host_ports_vec = storage->getWriteWorkers(nullptr, context);

        if constexpr (ReturnArray)
        {
            Array res;
            for (auto & host_ports : host_ports_vec)
                res.push_back((protocol == TCP) ? host_ports.getTCPAddress() : host_ports.getHTTPAddress());

            return result_type->createColumnConst(input_rows_count, res)->convertToFullColumnIfConst();
        }
        else
        {
            String res;
            if (!host_ports_vec.empty())
            {
                std::uniform_int_distribution dist;
                size_t i = dist(thread_local_rng) % host_ports_vec.size();
                auto host_ports = host_ports_vec[i];
                res = (protocol == TCP) ? host_ports.getTCPAddress() : host_ports.getHTTPAddress();
            }
            return result_type->createColumnConst(input_rows_count, res)->convertToFullColumnIfConst();
        }
    }

private:
    ContextPtr context;
};

struct NameGetWriteWorker { static constexpr auto name = "getWriteWorker"; };
struct NameGetWriteWorkers { static constexpr auto name = "getWriteWorkers"; };

using FunctionGetWriteWorker = FunctionGetWriteWorkerBase<NameGetWriteWorker, false>;
using FunctionGetWriteWorkers = FunctionGetWriteWorkerBase<NameGetWriteWorkers, true>;


void registerFunctionGetWriteWorker(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGetWriteWorker>();
    factory.registerFunction<FunctionGetWriteWorkers>();
}

}
