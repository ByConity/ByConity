#include <CloudServices/CnchDataAdapter.h>
#include <IO/ReadBufferFromString.h>
namespace DB {

String DeleteBitmapPlainTextAdapter::getPartitionId()
{
    if (!partition_id.empty())
        return partition_id;
    ReadBufferFromString in(name);
    while (!in.eof())
    {
        char c;
        readChar(c, in);
        if (c == '_')
            break;

        partition_id.push_back(c);
    }

    return partition_id;
}
}
