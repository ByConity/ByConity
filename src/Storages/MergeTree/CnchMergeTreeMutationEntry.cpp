#include <Storages/MergeTree/CnchMergeTreeMutationEntry.h>

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

void CnchMergeTreeMutationEntry::writeText(WriteBuffer & out) const
{
    out << "format version: 1\n"
        << "txn_id: " << txn_id.toUInt64() << "\n"
        << "commit_ts: " << commit_time.toUInt64() << "\n"
        << "storage_commit_ts: " << columns_commit_time.toUInt64() << "\n";

    out << "commands: ";
    commands.writeText(out);
    out << "\n";
}

void CnchMergeTreeMutationEntry::readText(ReadBuffer & in)
{
    UInt64 txn_id_, commit_ts_, columns_commit_ts_;
    in >> "format version: 1\n" >> "txn_id: " >> txn_id_ >> "\n" >> "commit_ts: " >> commit_ts_ >> "\n" >> "storage_commit_ts: "
        >> columns_commit_ts_ >> "\n";

    txn_id = txn_id_;
    commit_time = commit_ts_;
    columns_commit_time = columns_commit_ts_;
    in >> "commands: ";
    commands.readText(in);
    in >> "\n";
}

String CnchMergeTreeMutationEntry::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

CnchMergeTreeMutationEntry CnchMergeTreeMutationEntry::parse(const String & str)
{
    CnchMergeTreeMutationEntry res;

    ReadBufferFromString in(str);
    res.readText(in);
    assertEOF(in);

    return res;
}

bool CnchMergeTreeMutationEntry::isReclusterMutation() const
{
    if (commands.size() == 1 && commands[0].type==MutationCommand::Type::RECLUSTER)
        return true;
    else
        return false;
}

}
