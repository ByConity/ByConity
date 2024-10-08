#include <Interpreters/InterpreterAdviseQuery.h>

#include <Advisor/WorkloadTable.h>
#include <Advisor/Advisor.h>
#include <Core/Block.h>
#include <Core/QualifiedTableName.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTAdviseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Poco/Logger.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <boost/algorithm/string/replace.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
}

BlockIO InterpreterAdviseQuery::execute()
{
    Block advise_result{
        ColumnWithTypeAndName{std::make_shared<DataTypeString>(), "database"},
        ColumnWithTypeAndName{std::make_shared<DataTypeString>(), "table"},
        ColumnWithTypeAndName{std::make_shared<DataTypeString>(), "column"},
        ColumnWithTypeAndName{std::make_shared<DataTypeString>(), "advise_type"},
        ColumnWithTypeAndName{std::make_shared<DataTypeString>(), "original"},
        ColumnWithTypeAndName{std::make_shared<DataTypeString>(), "optimized"},
        ColumnWithTypeAndName{std::make_shared<DataTypeString>(), "status"}};
    Block optimized_ddls_result = advise_result.cloneEmpty();
    auto advise_result_columns = advise_result.mutateColumns();
    auto optimized_ddls_columns = optimized_ddls_result.mutateColumns();

    std::vector<String> queries = loadQueries();
    WorkloadTables tables = loadTables();

    auto start_watch = std::chrono::high_resolution_clock::now();
    Advisor advisor(query_ptr->as<const ASTAdviseQuery &>().type);
    auto advises = advisor.analyze(queries, getContext());
    auto stop_watch = std::chrono::high_resolution_clock::now();

    LOG_DEBUG(getLogger("InterpreterAdviseQuery"), "Analyze cost: {} ms",
              std::chrono::duration_cast<std::chrono::milliseconds>(stop_watch - start_watch).count());

    start_watch = std::chrono::high_resolution_clock::now();

    for (auto & advise : advises)
    {
        String result = advise->apply(tables);

        advise_result_columns[0]->insert(advise->getTable().database);
        advise_result_columns[1]->insert(advise->getTable().table);
        advise_result_columns[2]->insert(advise->getColumnName().value_or(""));
        advise_result_columns[3]->insert(advise->getAdviseType());
        advise_result_columns[4]->insert(advise->getOriginalValue());
        advise_result_columns[5]->insert(advise->getOptimizedValue());
        advise_result_columns[6]->insert(result);
    }

    stop_watch = std::chrono::high_resolution_clock::now();

    LOG_DEBUG(getLogger("InterpreterAdviseQuery"), "Apply advises cost: {} ms",
              std::chrono::duration_cast<std::chrono::milliseconds>(stop_watch - start_watch).count());

    if (query_ptr->as<const ASTAdviseQuery &>().output_ddl)
    {
        start_watch = std::chrono::high_resolution_clock::now();

        std::vector<std::pair<QualifiedTableName, String>> optimized_ddls = tables.getOptimizedDDLs();

        auto optimized_file = query_ptr->as<const ASTAdviseQuery &>().optimized_file;
        if (optimized_file)
        {
            std::vector<String> ddls;
            for (const auto & item : optimized_ddls)
                ddls.emplace_back(item.second);
            write(optimized_file.value(), ddls);
        }
        else
        {
            for (const auto & item : optimized_ddls)
            {
                advise_result_columns[0]->insert(item.first.database);
                advise_result_columns[1]->insert(item.first.table);
                advise_result_columns[2]->insert("");
                advise_result_columns[3]->insert("Optimized Create Table DDL");
                advise_result_columns[4]->insert("");
                advise_result_columns[5]->insert(item.second);
                advise_result_columns[6]->insert("");
            }
        }

        stop_watch = std::chrono::high_resolution_clock::now();

        LOG_DEBUG(
            getLogger("InterpreterAdviseQuery"),
            "Get optimal DDL cost: {} ms",
            std::chrono::duration_cast<std::chrono::milliseconds>(stop_watch - start_watch).count());
    }

    BlocksList blocks;
    advise_result.setColumns(std::move(advise_result_columns));
    blocks.push_back(std::move(advise_result));
    optimized_ddls_result.setColumns(std::move(optimized_ddls_columns));
    blocks.push_back(std::move(optimized_ddls_result));

    BlockIO res;
    res.in = std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
    return res;
}

static String readFile(const String & file_name)
{
    std::ifstream infile(file_name);
    String res((std::istreambuf_iterator<char>(infile)), std::istreambuf_iterator<char>());
    return res;
}

static String unescape(String query)
{
    boost::replace_all(query, "\\r", "\r");
    boost::replace_all(query, "\\n", "\n");
    boost::replace_all(query, "\\t", "\t");
    boost::replace_all(query, "\\\"", "\"");
    boost::replace_all(query, "\\'", "'");
    return query;
}

static std::vector<String> splitQueries(const String & s, const String & delimiter)
{
    std::vector<String> res;
    size_t last = 0;
    size_t next;
    while ((next = s.find(delimiter, last)) != String::npos)
    {
        auto query = s.substr(last, next - last);
        auto unescaped = unescape(query);
        res.push_back(unescaped);
        last = next + 1;
    }
    return res;
}

std::vector<String> InterpreterAdviseQuery::loadQueries()
{
    const auto & queries_file = query_ptr->as<const ASTAdviseQuery &>().queries_file;
    if (!queries_file)
        return {};
    return splitQueries(readFile(queries_file.value()), query_ptr->as<const ASTAdviseQuery &>().separator.value_or(";"));
}

WorkloadTables InterpreterAdviseQuery::loadTables()
{
    WorkloadTables res{Context::createCopy(getContext())};

    const auto & tables = query_ptr->as<const ASTAdviseQuery &>().tables;
    if (!tables)
        return res;

    if (auto * list = tables->as<ASTExpressionList>())
        for (const auto & table : list->getChildren())
        {
            if (auto * item = table->as<ASTQualifiedAsterisk>())
            {
                if (item->children.empty() || !item->getChildren()[0]->as<ASTTableIdentifier>())
                    throw Exception("Unable to resolve qualified asterisk", ErrorCodes::UNKNOWN_IDENTIFIER);
                ASTIdentifier & identifier = item->getChildren()[0]->as<ASTTableIdentifier &>();
                if (identifier.nameParts().size() == 2)
                    res.getTable(QualifiedTableName{identifier.nameParts()[0], identifier.nameParts()[1]});
                else if (identifier.nameParts().size() == 1)
                    res.loadTablesFromDatabase(identifier.nameParts()[0]);
                else
                    throw Exception("Unexpected table identifier", ErrorCodes::UNKNOWN_IDENTIFIER);
            }
            else if (auto * table_id = table->as<ASTTableIdentifier>())
                res.getTable(table_id->getTableId().getQualifiedName());
            else if (auto * identifier = table->as<ASTIdentifier>())
            {
                if (identifier->nameParts().size() == 2)
                    res.getTable(QualifiedTableName{identifier->nameParts()[0], identifier->nameParts()[1]});
                else if (identifier->nameParts().size() == 1)
                    res.getTable(QualifiedTableName{getContext()->getCurrentDatabase(), identifier->nameParts()[0]});
                else
                    throw Exception("Unexpected table identifier", ErrorCodes::UNKNOWN_IDENTIFIER);
            }
            else
                throw Exception("Unexpected table identifier", ErrorCodes::UNKNOWN_IDENTIFIER);
        }
    return res;
}

void InterpreterAdviseQuery::write(const String & output_file, std::vector<String> & queries)
{
    std::ofstream outfile(output_file);
    for (const auto & query : queries)
    {
        outfile << query;
        if (!query.ends_with(';'))
            outfile << ';';
        outfile << std::endl;
    }
    outfile.close();
}

}
