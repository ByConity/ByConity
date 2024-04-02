#include <Parsers/SQLFingerprint.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Optimizer/Rewriter/SQLFingerprintRewriter.h>
#include <Poco/DigestStream.h>
#include <Poco/MD5Engine.h>

namespace DB
{
String SQLFingerprint::generateIntermediateAST(
    const char * query_begin,
    const char * query_end,
    size_t max_query_size,
    size_t max_parser_depth,
    DialectType dialect_type)
{
    ParserQuery parser(query_end, ParserSettings::valueOf(dialect_type));
    auto ast = parseQuery(parser, query_begin, query_end, "", max_query_size, max_parser_depth);
    SQLFingerprintRewriter rewriter;
    Void context;
    auto rewrite_ast = rewriter.visitNode(ast, context);
    ASTQueryWithOutput::resetOutputASTIfExist(*rewrite_ast);
    return serializeAST(*rewrite_ast);
}

String SQLFingerprint::generate(
    const char * query_begin,
    const char * query_end,
    size_t max_query_size,
    size_t max_parser_depth,
    DialectType dialect_type)
{
    auto ast_str = generateIntermediateAST(query_begin, query_end, max_query_size, max_parser_depth, dialect_type);
    Poco::MD5Engine md5;
    Poco::DigestOutputStream outstr(md5);
    outstr << ast_str;
    outstr.flush();
    String fingerprint_md5_str = Poco::DigestEngine::digestToHex(md5.digest());

    return fingerprint_md5_str;
}

String SQLFingerprint::generateMD5(ASTPtr & ast)
{
    auto ast_str = generate(ast);
    Poco::MD5Engine md5;
    Poco::DigestOutputStream outstr(md5);
    outstr << ast_str;
    outstr.flush();
    String fingerprint_md5_str = Poco::DigestEngine::digestToHex(md5.digest());

    return fingerprint_md5_str;
}

String SQLFingerprint::generate(ASTPtr & ast)
{
    SQLFingerprintRewriter rewriter;
    Void context;
    auto cloned_ast = ast->clone();
    auto rewrite_ast = rewriter.visitNode(cloned_ast, context);
    ASTQueryWithOutput::resetOutputASTIfExist(*rewrite_ast);
    return serializeAST(*rewrite_ast);
}
}
