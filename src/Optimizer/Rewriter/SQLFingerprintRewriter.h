#include <memory>
#include <Optimizer/SimpleExpressionRewriter.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>
#include "Core/Types.h"
#include "Interpreters/StorageID.h"

namespace DB
{
class SQLFingerprintRewriter : public SimpleExpressionRewriter<Void>
{
public:
    static void rewriteAST(ASTPtr & node)
    {
        SQLFingerprintRewriter rewriter;
        Void dummy_context;
        node = rewriter.visitNode(node, dummy_context);
        ASTQueryWithOutput::resetOutputASTIfExist(*node);
    }

    ASTPtr visitASTLiteral(ASTPtr & node, Void & context) override
    {
        auto literal_ptr = std::dynamic_pointer_cast<ASTLiteral>(node);
        literal_ptr->updateField("?");
        return visitNode(node, context);
    }

    ASTPtr visitASTTableIdentifier(ASTPtr & node, Void & context) override
    {
        auto literal_ptr = std::dynamic_pointer_cast<ASTTableIdentifier>(node);
        literal_ptr->full_name = "?";
        literal_ptr->uuid = UUIDHelpers::Nil;
        return visitNode(node, context);
    }

    ASTPtr visitASTSelectQuery(ASTPtr & node, Void & context) override
    {
        auto select_ptr = std::dynamic_pointer_cast<ASTSelectQuery>(node);
        select_ptr->removeSettingsAndOutputFormat();
        return visitNode(node, context);
    }

};
}
