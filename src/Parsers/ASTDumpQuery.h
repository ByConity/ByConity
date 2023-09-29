#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

class ASTDumpQuery : public ASTQueryWithOutput
{
public:
    enum class Kind
    {
        DDL,
        Query,
        Workload,
    };

    enum class Expression : uint8_t
    {
        DATABASES,
        SUBQUERY,
        QUERY_IDS,
        BEGIN_TIME,
        END_TIME,
        WHERE,
        LIMIT_LENGTH,
        CLUSTER,
        DUMP_PATH,
    };

    String getID(char) const override;

    ASTType getType() const override { return ASTType::ASTDumpQuery; }

    ASTPtr clone() const override;

    void setExpression(Expression expr, ASTPtr && ast);

    ASTPtr queryIds()    const { return getExpression(Expression::QUERY_IDS); }
    ASTPtr subquery()    const { return getExpression(Expression::SUBQUERY); }
    ASTPtr beginTime()    const { return getExpression(Expression::BEGIN_TIME); }
    ASTPtr endTime()    const { return getExpression(Expression::END_TIME); }
    ASTPtr where()    const { return getExpression(Expression::WHERE); }
    ASTPtr databases()    const { return getExpression(Expression::DATABASES); }
    ASTPtr limitLength()    const { return getExpression(Expression::LIMIT_LENGTH); }
    ASTPtr cluster()    const { return getExpression(Expression::CLUSTER); }
    ASTPtr dumpPath()    const { return getExpression(Expression::DUMP_PATH); }

    ASTPtr getExpression(Expression expr, bool clone = false) const
    {
        auto it = positions.find(expr);
        if (it != positions.end())
            return clone ? children[it->second]->clone() : children[it->second];
        return {};
    }

    Kind kind;
    bool without_ddl{false};
protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
private:
    std::unordered_map<Expression, size_t> positions;

    ASTPtr & getExpression(Expression expr);
};

}
