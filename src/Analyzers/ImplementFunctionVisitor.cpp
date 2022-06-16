#include <Analyzers/ImplementFunctionVisitor.h>

#include <Common/StringUtils/StringUtils.h>
#include <Parsers/ASTAsterisk.h>
#include <Storages/StorageMaterializedView.h>

namespace DB
{

namespace ErrorCodes
{
}

void ImplementFunction::visit(ASTFunction & node, ASTPtr &)
{
    rewriteAsTranslateQualifiedNamesVisitorDo(node);
    rewriteAsQueryNormalizerDo(node);
}

void ImplementFunction::rewriteAsTranslateQualifiedNamesVisitorDo(ASTFunction & node)
{
    ASTPtr & func_arguments = node.arguments;

    if (!func_arguments) return;

    String func_name_lowercase = Poco::toLower(node.name);
    if (func_name_lowercase == "count" &&
        func_arguments->children.size() == 1 &&
        func_arguments->children[0]->as<ASTAsterisk>())
        func_arguments->children.clear();
}

void ImplementFunction::rewriteAsQueryNormalizerDo(ASTFunction &)
{
}

}
