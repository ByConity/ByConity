#include<Parsers/ASTPartToolKit.h>

namespace DB
{

String ASTPartToolKit::getID(char ) const
{
    return "PartToolKit";
}

ASTPtr ASTPartToolKit::clone() const
{
    auto res = std::make_shared<ASTPartToolKit>(*this);
    res->children.clear();

    if (data_format)
        res->data_format = data_format->clone();
    
    if (create_query)
        res->create_query = create_query->clone();

    if (source_path)
        res->source_path = source_path->clone();

    if (target_path)
        res->target_path = target_path->clone();

    if (settings)
        res->settings = settings->clone();

    return res;
}

void ASTPartToolKit::formatImpl(const FormatSettings & , FormatState & , FormatStateStacked ) const
{

}

}
