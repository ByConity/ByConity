#include <DataTypes/DataTypeFactory.h>

#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTExternalFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateFunctionQuery.h>

#include <Functions/UserDefined/ReservedNames.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_UDF_FUNCTION_NAME;
};

bool ParserCreateFunctionQuery::validateFunctionName(const String & name)
{
    for (const char & x : name)
    {
        if (!isWordCharASCII(x))
        {
            return false;
        }
    }
    return true;
}

static bool readDollarQuotedStringInto(String & body, String & tag, ReadBuffer & buf, size_t size)
{
    char * pos = buf.position();
    char * end = pos + size;

    if (*pos++ != '$')
        return false;

    char * tag_end = find_first_symbols<'$'>(pos, end);

    if (!tag_end)
        return false;

    tag = std::string_view(pos, tag_end - pos);
    pos = tag_end + 1;

    char * rear = strstr(pos, tag.c_str());

    if (!rear)
        return false;

    while (*--rear == ' ')
        ;

    while (*pos == ' ')
        pos++;

    body = std::string_view(pos, rear - pos);
    return true;
}

bool ParserCreateFunctionQuery::parseExternalUDF(Pos & pos, ASTPtr & node, Expected & expected, bool is_aggregate)
{
    ParserKeyword s_returns("RETURNS");
    ParserIdentifierWithOptionalParameters return_type_p;
    ParserKeyword s_flags("FLAGS");
    ParserNumber flags_p;
    ParserKeyword s_language("LANGUAGE");
    ParserIdentifier function_language_p;
    ParserKeyword s_as("AS");

    auto fn = std::make_shared<ASTExternalFunction>();

    if (is_aggregate)
        fn->args.flags |= FIELD_PREP(UDF_FIELD_TYPE, UDFFunctionType::Aggregate);

    if (s_returns.ignore(pos, expected))
    {
        ASTPtr return_type;

        if (!return_type_p.parse(pos, return_type, expected))
            return false;

        fn->args.ret = DataTypeFactory::instance().get(return_type);
    }

    if (s_flags.ignore(pos, expected))
    {
        ASTPtr udf_flags;
        UInt64 f;

        if (!flags_p.parse(pos, udf_flags, expected))
            return false;

        if (!udf_flags->as<ASTLiteral &>().value.tryGet(f))
            return false;

        if (f > USER_MAX_FLAG_VALUE)
            return false;

        fn->args.flags |= f;
    }
    else
    {
        fn->args.flags |= SCALAR_UDF_DEFAULT_FLAGS;
    }

    if (s_language.ignore(pos, expected))
    {
        ASTPtr function_language;

        if (!function_language_p.parse(pos, function_language, expected))
            return false;

        auto lang = function_language->as<ASTIdentifier &>().name();
        if (!setLangauge(fn->args.flags, lang.c_str()))
            return false;
    }

    if (!s_as.ignore(pos, expected))
        return false;

    ReadBufferFromMemory buf(pos->begin, pos->end - pos->begin);
    if (!readDollarQuotedStringInto(fn->body, fn->tag, buf, pos->size()))
        return false;

    auto tag = "$" + fn->tag + "$";

    do
    {
        ++pos;
        std::string s(std::string_view(pos->begin, pos->end - pos->begin));
    } while (std::string_view(pos->begin, pos->end - pos->begin) != tag);
    ++pos;

    node = fn;
    return true;
}

bool ParserCreateFunctionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_agg("AGGREGATE");
    ParserKeyword s_function("FUNCTION");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_on("ON");
    ParserIdentifier function_name_p;
    ParserKeyword s_as("AS");
    ParserExpression lambda_p;
    ParserToken s_dot(TokenType::Dot);

    ASTPtr database_name;
    ASTPtr function_name;
    ASTPtr function_core;

    String cluster_str;
    bool or_replace = false;
    bool if_not_exists = false;
    bool is_aggregate = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (s_agg.ignore(pos, expected))
        is_aggregate = true;

    if (!s_function.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!function_name_p.parse(pos, function_name, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database_name = function_name;
        if (!function_name_p.parse(pos, function_name, expected))
            return false;
    }

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    bool is_lambda = s_as.ignore(pos, expected);
    if (is_lambda)
    {
        if (is_aggregate)
            return false;

        if (!lambda_p.parse(pos, function_core, expected))
            return false;
    }
    else
    {
        if (!parseExternalUDF(pos, function_core, expected, is_aggregate))
            return false;
    }

    auto create_function_query = std::make_shared<ASTCreateFunctionQuery>();
    node = create_function_query;

    if (!tryGetIdentifierNameInto(function_name, create_function_query->function_name))
        return false;

    if (database_name)
    {
        if (!tryGetIdentifierNameInto(database_name, create_function_query->database_name))
            return false;
    }
    else if (isReservedName(create_function_query->function_name.c_str()))
    {
        return false;
    }

    const String & f_name = function_name->as<ASTIdentifier &>().full_name;
    if (!validateFunctionName(f_name))
    {
        throw Exception(
            "Function name can only contain alphanumeric and underscores. Invalid name - " + f_name,
            ErrorCodes::INCORRECT_UDF_FUNCTION_NAME);
    }
    create_function_query->function_name = f_name;
    create_function_query->function_core = function_core;
    create_function_query->children.push_back(function_core);

    create_function_query->or_replace = or_replace;
    create_function_query->if_not_exists = if_not_exists;
    create_function_query->is_lambda = is_lambda;
    create_function_query->cluster = std::move(cluster_str);

    return true;
}

}
