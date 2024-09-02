#include <Parsers/ASTPartition.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Parsers/ASTSerDerHelper.h>
#include <QueryPlan/PlanSerDerHelper.h>

namespace DB
{

String ASTPartition::getID(char delim) const
{
    if (value)
        return "Partition";
    else
        return "Partition_ID" + (delim + id);
}

ASTPtr ASTPartition::clone() const
{
    auto res = std::make_shared<ASTPartition>(*this);
    res->children.clear();

    if (value)
    {
        res->value = value->clone();
        res->children.push_back(res->value);
    }

    return res;
}

void ASTPartition::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (value)
    {
        value->formatImpl(settings, state, frame);
    }
    else
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "ID " << (settings.hilite ? hilite_none : "");
        WriteBufferFromOwnString id_buf;
        writeQuoted(id, id_buf);
        settings.ostr << id_buf.str();
    }
}

void ASTPartition::serialize(WriteBuffer & buf) const
{
    serializeAST(value, buf);
    writeBinary(fields_str, buf);
    writeBinary(fields_count, buf);
    writeBinary(id, buf);
}

void ASTPartition::deserializeImpl(ReadBuffer & buf)
{
    value = deserializeAST(buf);
    readBinary(fields_str, buf);
    readBinary(fields_count, buf);
    readBinary(id, buf);
}

ASTPtr ASTPartition::deserialize(ReadBuffer & buf)
{
    auto partition = std::make_shared<ASTPartition>();
    partition->deserializeImpl(buf);
    return partition;
}

}
