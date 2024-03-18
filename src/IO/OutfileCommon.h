#pragma once

#include <string>
#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Interpreters/Context.h>
#include "Interpreters/Context_fwd.h"


namespace DB
{

/***
 * OutfileTarget contains Outfile informations from Output AST 
 * like "SELECT ... INTO OUTFILE 'uri' FORMAT format  [ COMPRESSION 'compression' [ LEVEL  compression_level ] ]"
 * can create WriteBuffer for outfile with info to redirect data output
*/
class OutfileTarget
{
public:
    enum OutType
    {
        NO_OUT_FILE, // There is output but not file, like stdout
        SINGLE_OUT_FILE,
        MULTI_OUT_FILE
    };

    explicit OutfileTarget(
        const ContextMutablePtr & context_,
        std::string uri,
        std::string format = "",
        std::string compression_method_str = "",
        int compression_level = 1);

    void getRawBuffer();
    /// Generate desirable buffer according to schema and file out path
    /// New write buffer is owned by this object
    std::shared_ptr<WriteBuffer> getOutfileBuffer(bool allow_into_local = false);

    // Used to update buffer when export into multiple files(directory).
    // It constructs new buffer in-place, and the shared_ptr of first buffer will be keep to make sure
    // the address will be and only be destructed at last.
    // Why we do this dangerous behaviour is that out buffer used in IOutputFormat is a reference parameter,
    // and it can only refer to the first buffer and not be changed to another buffer.
    WriteBuffer * updateBuffer();

    bool outToFile() const { return out_type != OutType::NO_OUT_FILE; }
    bool outToMultiFile() const { return out_type == OutType::MULTI_OUT_FILE; }

    void accumulateBytes(size_t new_bytes) { current_bytes += new_bytes; }
    bool needSplit() const { return out_type == MULTI_OUT_FILE && current_bytes > split_file_limit; }

    /// Send buf data to remote end, for tos and hdfs, just flush the buffer, not release the buffer
    void flushFile();
    void updateOutPathIfNeeded();

    void resetCounter() { current_bytes = 0; serial_no = 1; }

    static void setOutfileCompression(
        const ASTQueryWithOutput * query_with_output, String & outfile_compression_method_str, UInt64 & outfile_compression_level);

    // return true if it can export on server with tcp
    static bool checkOutfileWithTcpOnServer(const ContextMutablePtr & context);

private:
    ContextMutablePtr context;

    OutType out_type{NO_OUT_FILE};
    // for serialize and deserialize
    std::string request_uri;
    std::string real_outfile_path;

    String scheme;     // What the protocol's scheme is in remote output file.
    std::string format;
    std::string compression_method_str{}; // origin compression_method_str
    CompressionMethod compression_method;
    int compression_level;

    /// The following members are only used to output to multi files
    int serial_no{1}; // Used in multi out file mode to generate file name.
    size_t current_bytes{0}; // It is used to limit file size.
    size_t split_file_limit{0}; // File split threshold set by user query, or default value.

    std::unique_ptr<WriteBuffer> out_buf_raw;
    std::shared_ptr<WriteBuffer> out_buf;
};

}
