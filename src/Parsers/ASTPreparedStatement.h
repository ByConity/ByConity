#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/IAST.h>

namespace DB
{

class ASTCreatePreparedStatementQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String name;
    ASTPtr query;

    bool if_not_exists = false;
    bool or_replace = false;
    bool is_permanent = false;

    /** Get the text that identifies this element. */
    String getID(char) const override
    {
        return "CreatePreparedStatement";
    }

    ASTType getType() const override
    {
        return ASTType::ASTCreatePreparedStatementQuery;
    }

    ASTPtr clone() const override;

    String getName() const
    {
        return name;
    }
    ASTPtr getQuery() const
    {
        return query;
    }

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override
    {
        return removeOnCluster<ASTCreatePreparedStatementQuery>(clone());
    }

protected:
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};

class ASTExecutePreparedStatementQuery : public ASTQueryWithOutput
{
public:
    String name;
    ASTPtr values;


    /** Get the text that identifies this element. */
    String getID(char) const override
    {
        return "ExecutePreparedStatement";
    }

    ASTType getType() const override
    {
        return ASTType::ASTExecutePreparedStatementQuery;
    }

    ASTPtr clone() const override;

    const String & getName() const
    {
        return name;
    }
    ASTPtr getValues() const
    {
        return values;
    }

protected:
    void formatQueryImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};

class ASTShowPreparedStatementQuery : public ASTQueryWithOutput
{
public:
    String name;

    bool show_create = false;
    bool show_explain = false;

    /** Get the text that identifies this element. */
    String getID(char) const override
    {
        return "ShowPreparedStatement";
    }

    ASTType getType() const override
    {
        return ASTType::ASTShowPreparedStatementQuery;
    }

    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};

class ASTDropPreparedStatementQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String name;

    bool if_exists = false;

    /** Get the text that identifies this element. */
    String getID(char) const override
    {
        return "DropPreparedStatement";
    }

    ASTType getType() const override
    {
        return ASTType::ASTDropPreparedStatementQuery;
    }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override
    {
        return removeOnCluster<ASTDropPreparedStatementQuery>(clone());
    }

protected:
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};
}
