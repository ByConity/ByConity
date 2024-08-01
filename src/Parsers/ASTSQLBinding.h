#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/IAST.h>

namespace DB
{
enum class BindingLevel
{
    SESSION,
    GLOBAL
};

class ASTCreateBinding : public IAST
{
public:
    enum class Expression : uint8_t
    {
        QUERY_PATTERN,
        TARGET,
        SETTINGS
    };

    BindingLevel level;
    String query_pattern;
    String re_expression;
    ASTPtr pattern;
    ASTPtr target;
    ASTPtr settings;
    bool if_not_exists = false;
    bool or_replace = false;


    /** Get the text that identifies this element. */
    String getID(char) const override { return "CreateBinding"; }

    ASTType getType() const override { return ASTType::ASTCreateBinding; }

    ASTPtr clone() const override;

    void setExpression(Expression expr, ASTPtr && ast);
    ASTPtr getExpression(Expression expr, bool clone = false) const;

    ASTPtr getSettings() const { return settings; }
    ASTPtr getPattern() const { return pattern; }
    ASTPtr getTarget() const { return target; }

protected:
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;

// private:
//     std::unordered_map<Expression, size_t> positions;
};

class ASTShowBindings : public IAST
{
public:
    /** Get the text that identifies this element. */
    String getID(char) const override { return "ShowBindings"; }

    ASTType getType() const override { return ASTType::ASTShowBindings; }

    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};

class ASTDropBinding : public IAST
{
public:
    String uuid;
    String re_expression;
    String pattern;
    ASTPtr pattern_ast;
    BindingLevel level;
    bool if_exists{false};
    /** Get the text that identifies this element. */
    String getID(char) const override { return "DropBinding"; }

    ASTType getType() const override { return ASTType::ASTDropBinding; }

    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};

}
