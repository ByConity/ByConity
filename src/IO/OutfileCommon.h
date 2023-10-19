#pragma once

#include <map>
#include <memory>
#include <string>
#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Interpreters/Context.h>
#include "Interpreters/Context_fwd.h"
#include "WriteBuffer.h"


namespace DB
{
    
class OutfileTarget;
using OutfileTargetPtr = std::shared_ptr<OutfileTarget>;

/***
 * OutfileTarget contians Outfile informations from Output AST 
 * like "SELECT ... INTO OUTFILE 'uri' FORMAT format  [ COMPRESSION 'compression' [ LEVEL  compression_level ] ]"
 * can create WriteBuffer for outfile with info to redirect data output
*/
class OutfileTarget
{
public:
    // for serialize and deserialize
    std::string uri;
    std::string format;
    std::string compression_method_str;
    int compression_level;

    explicit OutfileTarget(
        std::string uri,
        std::string format = "",
        std::string compression_method_str = "",
        int compression_level = 1);

    OutfileTarget(const OutfileTarget& outfile_target);

    std::shared_ptr<WriteBuffer> getOutfileBuffer(const ContextPtr & context , bool allow_into_local = false);
    
    void flushFile();

    static OutfileTargetPtr getOutfileTarget(
        const std::string & uri,
        const std::string & format = "",
        const std::string & compression_method_str = "",
        int compression_level = 1);

    static void setOufileCompression(
        const ASTQueryWithOutput * query_with_output, String & outfile_compression_method_str, UInt64 & outfile_compression_level);

    // return true if it can export on server with tcp
    static bool checkOutfileWithTcpOnServer(const ContextMutablePtr & context);

private:
    std::unique_ptr<WriteBuffer> out_buf_raw;
    std::shared_ptr<WriteBuffer> out_buf;
};

}
