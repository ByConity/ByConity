#include <boost/program_options/variables_map.hpp>
#include "DataTypes/DataTypeLowCardinality.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/IDataType.h"
#include "Formats/FormatFactory.h"
#include "IO/WriteBufferFromFileDescriptor.h"
#include "Interpreters/DatabaseAndTableWithAlias.h"
#include "Interpreters/ExpressionAnalyzer.h"
#include "Parsers/queryToString.h"
#include "Processors/Formats/Impl/LMNativeORCBlockInputFormat.h"
#include "QueryPlan/IQueryPlanStep.h"
#include "Storages/SelectQueryInfo.h"
// #include<iostream>
#include <cstdint>
#include <memory>
#include <vector>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseMemory.h>
#include <Formats/FormatFactory.cpp>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Interpreters/TreeRewriter.cpp>
#include <Optimizer/SelectQueryInfoHelper.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Formats/Impl/OrcChunkReader.h>
#include <Storages/StorageMemory.h>
#include <boost/program_options.hpp>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/Path.h>
#include <Poco/PatternFormatter.h>
#include <Common/Stopwatch.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <common/logger_useful.h>
using namespace ::DB;

void initLogger(const String & level)
{
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%Y.%m.%d %H:%M:%S.%F <%p> %s: %t"));
    Poco::AutoPtr<Poco::ConsoleChannel> console_chanel(new Poco::ConsoleChannel);
    Poco::AutoPtr<Poco::FormattingChannel> channel(new Poco::FormattingChannel(formatter, console_chanel));
    Poco::Logger::root().setLevel(level);
    Poco::Logger::root().setChannel(channel);
}

namespace po = boost::program_options;
po::variables_map setArgs(int argc, char * argv[])
{
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "print help message")("logging_level", po::value<String>()->default_value("trace"), "logging level")(
        "limit,l", po::value<int64_t>()->default_value(100), "line limit");
    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    return options;
}

SelectQueryInfo mockInfo(const ASTPtr & query, const Block & header, ContextPtr context)
{
    SelectQueryInfo query_info;
    query_info.query = query;

    const auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AST type found in buildSelectQueryInfoForQuery");

    StoragePtr storage;
    StorageMetadataPtr metadata_snapshot;

    const auto * table_expression = getTableExpression(*select_query, 0);
    if (table_expression && table_expression->database_and_table_name)
    {
        storage = DatabaseCatalog::instance().getTable(StorageID{table_expression->database_and_table_name}, context);
        metadata_snapshot = storage->getInMemoryMetadataPtr();
    }

    // fill syntax_analyzer_result
    query_info.syntax_analyzer_result
        = TreeRewriter(context).analyzeSelect(query_info.query, TreeRewriterResult({}, storage, metadata_snapshot));

    // fill prepared_set
    auto query_analyzer
        = std::make_unique<SelectQueryExpressionAnalyzer>(query_info.query, query_info.syntax_analyzer_result, context, metadata_snapshot);

    query_analyzer->makeSetsForIndex(select_query->where());
    query_analyzer->makeSetsForIndex(select_query->prewhere());
    query_info.sets = std::move(query_analyzer->getPreparedSets());
    if (select_query->where())
    {
        auto where = select_query->where();
        auto required_columns = SymbolsExtractor::extract(where);
        std::vector<String> required_names(required_columns.begin(), required_columns.end());
        auto input_block = metadata_snapshot->getSampleBlockForColumns(required_names);


        auto action = IQueryPlanStep::createFilterExpressionActions(context, select_query->where(), input_block);
        std::cout << "actions: " << action->dumpDAG() << std::endl;
        std::cout << "actions name:  " << select_query->where()->getColumnName() << std::endl;
        std::cout << "actions query:" << queryToString(select_query->where()) << std::endl;
        query_info.prewhere_info = std::make_shared<PrewhereInfo>(action, select_query->where()->getColumnName());
        query_info.prewhere_info->need_filter = true;
    }
    return query_info;
}

SelectQueryInfo buildQueryInfo(const String & query, const Block & header, ContextPtr context)
{
    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query, 1000, 1000);
    auto ret = mockInfo(ast, header, context);

    // mock prewhere
    return ret;
}

struct State
{
    State(const State &) = delete;

    ContextMutablePtr context;

    static const State & instance()
    {
        static State state;
        return state;
    }

    const NamesAndTypesList & getColumns() const { return tables[0].columns; }

    std::vector<TableWithColumnNamesAndTypes> getTables(size_t num = 0) const
    {
        std::vector<TableWithColumnNamesAndTypes> res;
        for (size_t i = 0; i < std::min(num, tables.size()); ++i)
            res.push_back(tables[i]);
        return res;
    }

private:
    static DatabaseAndTableWithAlias createDBAndTable(String table_name)
    {
        DatabaseAndTableWithAlias res;
        res.database = "test";
        res.table = table_name;
        return res;
    }
    DataTypePtr lc_string_type
        = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
    const std::vector<TableWithColumnNamesAndTypes> tables{
        TableWithColumnNamesAndTypes(
            createDBAndTable("call_center"),
            {{"lo_linenumber", std::make_shared<DataTypeInt64>()}, {"c_nation", lc_string_type}, {"c_region", lc_string_type}}),
        // TableWithColumnNamesAndTypes(
        //     createDBAndTable("table"),
        //     {
        //         {"column", std::make_shared<DataTypeUInt8>()},
        //         {"apply_id", std::make_shared<DataTypeUInt64>()},
        //         {"apply_type", std::make_shared<DataTypeUInt8>()},
        //         {"apply_status", std::make_shared<DataTypeUInt8>()},
        //         {"create_time", std::make_shared<DataTypeDateTime>()},
        //         {"field", std::make_shared<DataTypeString>()},
        //         {"value", std::make_shared<DataTypeString>()},
        //     }),
        // TableWithColumnNamesAndTypes(
        //     createDBAndTable("table2"),
        //     {
        //         {"num", std::make_shared<DataTypeUInt8>()},
        //         {"attr", std::make_shared<DataTypeString>()},
        //     }),
    };

    explicit State() : context(Context::createCopy(getContext().context))
    {
        tryRegisterFunctions();
        tryRegisterFormats();
        DatabasePtr database = std::make_shared<DatabaseMemory>("test", context);

        for (const auto & tab : tables)
        {
            const auto & table_name = tab.table.table;
            const auto & db_name = tab.table.database;
            database->attachTable(
                table_name,
                StorageMemory::create(
                    StorageID(db_name, table_name),
                    ColumnsDescription{getColumns()},
                    ConstraintsDescription{},
                    ForeignKeysDescription{},
                    UniqueNotEnforcedDescription{},
                    String{}));
        }
        DatabaseCatalog::instance().attachDatabase(database->getDatabaseName(), database);
        context->setCurrentDatabase("test");
    }
};

Block buildHeader()
{
    Block header;
    header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeInt64>(), "lo_linenumber"));
    auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
    header.insert(ColumnWithTypeAndName(type, "c_nation"));
    header.insert(ColumnWithTypeAndName(type, "c_region"));
    return header;
}

void printBlock(const State & state, Block & block, WriteBuffer & wb)
{
    auto output_format = FormatFactory::instance().getOutputFormat("CSV", wb, block.cloneEmpty(), state.context);
    output_format->write(block);
    output_format->flush();
}

void readBySelect(const String & query_str, const String & file_name, size_t limit)
{
    initLogger("trace");
    auto log = getLogger(__PRETTY_FUNCTION__);
    const State & state = State::instance();
    ReadBufferFromFile rb(file_name);

    Block header = buildHeader();
    SelectQueryInfo query_info = buildQueryInfo(query_str, header, state.context);
    ScanParams scan_params{
        .header = header,
        .in = &rb,
        .select_query_info = &query_info,
        .local_context = state.context,
        .format_settings = {},
        .range_start = 0,
        .range_length = rb.getFileSize(),
        .chunk_size = 15};
    OrcScanner scanner(scan_params);
    scanner.init();

    Block block;
    size_t nread = 0;
    for (;;)
    {
        Status status_read = scanner.readNext(block);
        if (!status_read.ok())
        {
            LOG_INFO(getLogger(__PRETTY_FUNCTION__), "exit via {}", status_read.ToString());
            return;
        }
        LOG_DEBUG(log, block.dumpStructure());
        // WriteBufferFromFileDescriptor
        WriteBufferFromFileDescriptor wb(fileno(stdout));
        printBlock(state, block, wb);
        nread += block.rows();
        if (nread >= limit)
            break;
    }
}


// void readByFormat( const String & file_name)
// {
//     ReadBufferFromFile rb(file_name);
//     Block header = buildHeader();
//     LMNativeORCBlockInputFormat format(rb,header, {});
//     // format.setContext(state.context);
//     format.prepare();
//     format.work();

// }

int main(int argc, char * argv[])
{
    po::variables_map args = setArgs(argc, argv);
    initLogger(args["logging_level"].as<String>());
    String file = "/data01/rmq/transmit/ssb_flat_sample";
    // readByFormat(file);
    readBySelect("select  lo_linenumber, c_region, c_nation from test.call_center ", file, 100);
    readBySelect(
        "select  lo_linenumber, c_region, c_nation from test.call_center where lo_linenumber < 10 and lo_linenumber > 4", file, 100);

    return 0;
}