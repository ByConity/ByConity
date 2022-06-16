#include <Optimizer/Utils.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/formatAST.h>

#include <gtest/gtest.h>

using namespace DB;

void checkAstTreeEquals(const ASTPtr & left, const ASTPtr & right, bool expect)
{
    if (expect != Utils::astTreeEquals(left, right))
        GTEST_FAIL() << "AST equal check fails, left: " << serializeAST(*left) << ", right: " << serializeAST(*right)
                     << ", expect: " << expect << std::endl;
}

TEST(OptimizerUtils, AstTreeEquals)
{
    using std::make_shared;

    checkAstTreeEquals(make_shared<ASTLiteral>("a"), make_shared<ASTIdentifier>("a"), false);
    checkAstTreeEquals(make_shared<ASTLiteral>(1L), make_shared<ASTLiteral>(1U), false);
    checkAstTreeEquals(makeASTFunction("foo"), makeASTFunction("bar"), false);
    checkAstTreeEquals(makeASTFunction("foo"), makeASTFunction("foo"), true);
    checkAstTreeEquals(makeASTFunction("foo", make_shared<ASTLiteral>(1L)), makeASTFunction("foo", make_shared<ASTLiteral>(1U)), false);
    checkAstTreeEquals(makeASTFunction("foo", make_shared<ASTLiteral>(1L)), makeASTFunction("foo", make_shared<ASTLiteral>(1L)), true);
    checkAstTreeEquals(
        makeASTFunction("foo", make_shared<ASTLiteral>(1L), make_shared<ASTLiteral>(2L)),
        makeASTFunction("foo", make_shared<ASTLiteral>(2L), make_shared<ASTLiteral>(1L)),
        false);
    checkAstTreeEquals(
        makeASTFunction("foo", make_shared<ASTLiteral>(1L), makeASTFunction("bar", make_shared<ASTIdentifier>("x"))),
        makeASTFunction("foo", make_shared<ASTLiteral>(1L), makeASTFunction("bar", make_shared<ASTIdentifier>("x"))),
        true);
}
