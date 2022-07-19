#include "InterpreterCreateStatsQuery.h"
#include <Protos/optimizer_statistics.pb.h>
#include "Common/Stopwatch.h"
#include "Columns/ColumnsNumber.h"
#include "DataStreams/IBlockInputStream.h"
#include "DataTypes/DataTypeString.h"
#include "DataTypes/DataTypesNumber.h"
#include "Parsers/ASTStatsQuery.h"
#include "Statistics/StatisticsCollector.h"
#include "Statistics/StatsTableBasic.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

using namespace Statistics;

template <typename QueryType>
static auto getTableIdentifier(ContextPtr context, const QueryType * query)
{
    std::vector<StatsTableIdentifier> tables;
    auto catalog = createCatalogAdaptor(context);
    if (query->target_all)
    {
        auto db = context->getCurrentDatabase();
        return catalog->getAllTablesID(db);
    }
    else
    {
        auto db = context->resolveDatabase(query->database);
        auto table_info_opt = catalog->getTableIdByName(db, query->table);
        if (!table_info_opt)
        {
            auto msg = "Unknown Table (" + query->table + ") in database (" + db + ")";
            throw Exception(msg, ErrorCodes::UNKNOWN_TABLE);
        }
        tables.emplace_back(table_info_opt.value());
    }
    return tables;
}


static Block constructInfoBlock(ContextPtr context, const String & table_name, int64_t row_count, double time)
{
    Block block;
    auto append_str_column = [&](String header, String value) {
        ColumnWithTypeAndName tuple;
        tuple.name = header;
        tuple.type = std::make_shared<DataTypeString>();
        auto col = tuple.type->createColumn();
        col->insertData(value.data(), value.size());
        tuple.column = std::move(col);
        block.insert(std::move(tuple));
    };

    auto append_num_column = [&]<typename T>(String header, T value) {
        static_assert(std::is_trivial_v<T>);
        ColumnWithTypeAndName tuple;
        tuple.name = header;
        tuple.type = std::make_shared<DataTypeNumber<T>>();
        auto col = ColumnVector<T>::create();
        col->insertValue(value);
        tuple.column = std::move(col);
        block.insert(std::move(tuple));
    };

    append_str_column("table_name", table_name);
    append_num_column("row_count", static_cast<int64_t>(row_count));
    if (context->getSettingsRef().create_stats_time_output)
    {
        append_num_column("elapsed_time", time);
    }

    return block;
}


namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

// @return: row_count, elapsed_time
std::tuple<Int64, double> collectStatsOnTable(ContextPtr context, const StatsTableIdentifier & table_info)
{
    auto & logger = Poco::Logger::get("CreateStats");
    Stopwatch watch;
    try
    {
        auto catalog = createCatalogAdaptor(context);
        StatisticsCollector impl(context, catalog, table_info, 0);
        impl.collectFull();

        impl.writeToCatalog();
        auto row_count = impl.getTableStats().basic->getRowCount();
        auto elapsed_time = watch.elapsedSeconds();

        return std::make_tuple(row_count, elapsed_time);
    }
    catch (Exception & e)
    {
        logger.warning(e.displayText());
    }
    catch (std::exception & e)
    {
        logger.warning(e.what());
    }
    catch (...)
    {
        logger.warning("unknown error");
    }
    auto elapsed_time = watch.elapsedSeconds();
    return std::make_tuple(0, elapsed_time);
}

namespace
{
    class CreateStatsBlockInputStream : public IBlockInputStream, WithContext
    {
    public:
        CreateStatsBlockInputStream(ContextPtr context_, std::vector<StatsTableIdentifier> tables_)
            : WithContext(context_), tables(std::move(tables_))
        {
            timestamp = 0;
        }

        String getName() const override { return "Statistics"; }
        Block getHeader() const override { return {}; }

    private:
        Block readImpl() override
        {
            auto context = getContext();
            if (counter >= tables.size())
            {
                return {};
            }
            auto table_identifier = tables.at(counter);

            auto [row_count, elapsed_time] = collectStatsOnTable(context, table_identifier);
            ++counter;
            return constructInfoBlock(context, table_identifier.getTableName(), row_count, elapsed_time);
        }

    private:
        std::vector<StatsTableIdentifier> tables;
        size_t counter = 0;
        TxnTimestamp timestamp;
    };

}

BlockIO InterpreterCreateStatsQuery::execute()
{
    auto context = getContext();
    auto query = query_ptr->as<const ASTCreateStatsQuery>();
    if (!query)
    {
        throw Exception("Create stats query logical error", ErrorCodes::LOGICAL_ERROR);
    }

    auto tables = getTableIdentifier(context, query);
    std::vector<StatsTableIdentifier> valid_tables;
    auto catalog = createCatalogAdaptor(context);
    // TODO: check more for support
    for (const auto & table : tables)
    {
        if (catalog->isTableCollectable(table))
        {
            if (query->if_not_exists && catalog->hasStatsData(table))
            {
                // skip when if_not_exists is on
                continue;
            }
            valid_tables.emplace_back(table);
        }
    }
    BlockIO io;
    io.in = std::make_shared<CreateStatsBlockInputStream>(getContext(), std::move(valid_tables));
    return io;
}

}
