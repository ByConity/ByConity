#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>


namespace DB
{
/**
 * ADVISE
 *   [TABLES db1.* [,db2.* ,...]]
 *   [QUERIES '.../queries.sql' [SEPARATOR ';']]
 *   [AS OPTIMIZED '.../optimized.sql']
 */
class ASTAdviseQuery : public IAST
{
public:
    enum class AdvisorType
    {
        ALL,
        ORDER_BY,
        CLUSTER_BY,
        COLUMN_USAGE,
        DATA_TYPE,
        MATERIALIZED_VIEW,
        PROJECTION
    };
    AdvisorType type = AdvisorType::ALL;
    ASTPtr tables;
    std::optional<String> queries_file;
    std::optional<String> separator;
    bool output_ddl = false;
    std::optional<String> optimized_file;

    String getID(char) const override { return "AdviseQuery"; }

    ASTPtr clone() const override { return std::make_shared<ASTAdviseQuery>(*this); }

    ASTType getType() const override { return ASTType::ASTAdviseQuery; }

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    static const char * getTypeString(AdvisorType adviseType);
};

}
