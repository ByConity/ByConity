#pragma once

#include <assert.h>

#include <Common/config.h>
#include <Common/Throttler.h>
#include <Common/formatIPv6.h>
#include <IO/ReadBuffer.h>
#include <Poco/URI.h>
#include <Poco/String.h>
#include <IO/ReadBufferFromFileBase.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Poco/Net/IPAddress.h>

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

#if USE_HDFS
#include <hdfs/hdfs.h>
#include <consul/bridge.h>
#include <random>
namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
        extern const int NETWORK_ERROR;
        extern const int CANNOT_OPEN_FILE;
        extern const int POSITION_OUT_OF_BOUND;
    }


    /** Accepts path to file and opens it, or pre-opened file descriptor.
     * Closes file by himself (thus "owns" a file descriptor).
     */
    class ReadBufferFromByteHDFS : public ReadBufferFromFileBase
    {
        protected:
            String hdfs_name;
            String fuzzy_hdfs_uri;
            String uriPrefix;
            Strings hdfs_files;
            bool read_all_once;

            String hdfs_user;
            String hdfs_nnproxy;
            size_t current_pos;

            struct hdfsBuilder *builder;
            hdfsFS fs;
            hdfsFile fin;
            ThrottlerPtr total_network_throttler;
        public:
            ReadBufferFromByteHDFS(const String & hdfs_name_, const String hdfs_user_ = "clickhouse", const String nnproxy_ = "nnproxy", size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
                char * existing_memory = nullptr, size_t alignment = 0, bool read_all_once_ = false, ThrottlerPtr total_network_throttler_ = nullptr)
                : ReadBufferFromFileBase(buf_size, existing_memory, alignment),
                  hdfs_name(hdfs_name_), fuzzy_hdfs_uri(hdfs_name_), hdfs_files{}, read_all_once(read_all_once_),
                  hdfs_user(hdfs_user_), hdfs_nnproxy(nnproxy_),
                  current_pos(0), builder{nullptr}, fs{nullptr}, fin{nullptr}, total_network_throttler(total_network_throttler_)
            {
                String fuzzyFileNames;
                uriPrefix = hdfs_name_.substr(0, fuzzy_hdfs_uri.find_last_of('/'));
                if (uriPrefix.length() == fuzzy_hdfs_uri.length())
                {
                    fuzzyFileNames = fuzzy_hdfs_uri;
                    uriPrefix.clear();
                }
                else
                {
                    uriPrefix += "/";
                    fuzzyFileNames = fuzzy_hdfs_uri.substr(uriPrefix.length());
                }

                Poco::URI uri(uriPrefix); // not include file name

                auto host = uri.getHost();
                auto port = uri.getPort();

                Strings fuzzyNameList = parseDescription(fuzzyFileNames, 0, fuzzyFileNames.length(), ',' , 100/* hard coded max files */);
                std::vector<Strings > fileNames;
                for(auto fuzzyName : fuzzyNameList)
                fileNames.push_back(parseDescription(fuzzyName, 0, fuzzyName.length(), '|', 100));
                for (auto & vecNames : fileNames)
                {
                    for(auto & name : vecNames)
                    {
                        hdfs_files.push_back(std::move(name));
                    }
                }

                if (host.empty() || port == 0)
                {
                    std::tie(host, port) = getNameNodeNNProxy(hdfs_nnproxy);
                }
                else
                {
                    host = normalizeHost(host);
                }

                if (hdfs_files.size() == 0)
                {
                    throw Exception("Illegal HDFS URI : " + fuzzy_hdfs_uri, ErrorCodes::BAD_ARGUMENTS);
                }

                builder = hdfsNewBuilder();
                if (builder == nullptr)
                    throw Exception("Unable to create HDFS builder, maybe hdfs3.xml missing" , ErrorCodes::BAD_ARGUMENTS);
                // set read/connect timeout, default value in libhdfs3 is about 1 hour, and too large
                hdfsBuilderConfSetStr(builder, "input.read.timeout", "60000"); // 1 min
                hdfsBuilderConfSetStr(builder, "input.connect.timeout", "60000"); // 1 min

                hdfsBuilderSetNameNode(builder, host.c_str());
                hdfsBuilderSetNameNodePort(builder, port);
                hdfsBuilderSetUserName(builder, hdfs_user.c_str());
            }

            // ReadBufferFromByteHDFS(ReadBufferFromByteHDFS &&) = default;

            ~ReadBufferFromByteHDFS() override
            {
                close();
                hdfsDisconnect(fs);
                hdfsFreeBuilder(builder);
            }

            /// Close HDFS connection before destruction of object.
            void close()
            {
                hdfsCloseFile(fs, fin);
                fin = nullptr;
            }

            void assertValidBuffer(int offset, int size)
            {
                if (internal_buffer.begin() + offset >= internal_buffer.begin() &&
                    internal_buffer.begin() + offset + size <= internal_buffer.end())
                    return;
                throw Exception("Invalid ReadBufferFromByteHDFS offset: " + std::to_string(offset) + " size:" + std::to_string(size), ErrorCodes::POSITION_OUT_OF_BOUND);
            }

            bool nextImpl() override
            {
                int offset = 0;
                int size = internal_buffer.size();
                // If hdfs connection is not trigger, do it now
                tryConnect();

                do
                {
                    assertValidBuffer(offset, size);
                    int done = hdfsRead(fs, fin, internal_buffer.begin() + offset, size);
                    if (done <0)
                        throw Exception("Fail to read HDFS file: " + hdfs_files[current_pos] + " " + String(hdfsGetLastError()), ErrorCodes::CANNOT_OPEN_FILE);

                    if (done)
                    {
                        if (total_network_throttler)
                        {
                            total_network_throttler->add(done);
                        }
                        if (!read_all_once) {
                            working_buffer.resize(done);
                            break;
                        }
                        else
                        {
                            offset += done;
                            size -= done;
                            if (size > 0)
                            {
                                continue;
                            }
                            else
                            {
                                working_buffer.resize(offset);
                                break;
                            }
                        }
                    }
                    else
                    {
                        // reach eof of one file, go to next file if there is
                        if (current_pos < hdfs_files.size() -1)
                        {
                            ++current_pos;
                            close(); // close current hdfs file
                            Poco::URI uri(uriPrefix + hdfs_files[current_pos]);
                            auto& path = uri.getPath();
                            int retry = 0;
                            do
                            {
                                fin = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
                                if (retry++ > 1) break; // retry one more time in case namenode instable
                            } while (fin == nullptr);
                        }
                        else
                        {
                            return false;
                        }
                    }

                } while (true);

                return true;
            }

            const std::string& getHDFSUri() const
            {
                return fuzzy_hdfs_uri;
            }

            /// Get the position of the file corresponding to the data in buffer.
            off_t getPosition() override
            {
                tryConnect();
                off_t pos_in_file = hdfsTell(fs, fin);
                if (pos_in_file == -1)
                   throw Exception("Fail to getPosition HDFS file: " + hdfs_files[current_pos] + " " + String(hdfsGetLastError()), ErrorCodes::CANNOT_OPEN_FILE);
                return pos_in_file - (working_buffer.end() - pos);
            }

            // int getFD() const override
            // {
            //     return -1;
            // }

        protected:
            std::string getFileName() const override
            {
                return hdfs_name;
            }

        private:
            void tryConnect()
            {
                if (fs == nullptr)
                {
                    int retry = 0;
                    do
                    {
                        fs = hdfsBuilderConnect(builder);
                        if (fs == nullptr)
                        {
                            // NameNode cannot connect, record it in blacklist
                            auto host = hdfsBuilderGetNameNode(builder);
                            brokenNNs.insert(host);

                            if (retry++ > 2) throw Exception("Unable to connect to HDFS:" + String(hdfsGetLastError()), ErrorCodes::NETWORK_ERROR);
                            auto hostAndPort = getNameNodeNNProxy(hdfs_nnproxy);
                            // In case namenode is down, retry one
                            hdfsBuilderSetNameNode(builder, hostAndPort.first.c_str());
                            hdfsBuilderSetNameNodePort(builder, hostAndPort.second);
                        }
                    } while (fs == nullptr);

                    retry = 0;
                    Poco::URI uri(uriPrefix + hdfs_files[current_pos]);
                    auto& path = uri.getPath();
                    do
                    {
                        fin = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
                        if (retry++ > 1) break; // retry one more time in case namenode instable
                    } while (fin == nullptr);
                }
            }

            off_t seek(off_t offset, int whence) override
            {
                if (whence != SEEK_SET)
                    throw Exception("ReadBufferFromByteHDFS::seek expects SEEK_SET as whence", ErrorCodes::CANNOT_OPEN_FILE);

                tryConnect();

                off_t new_pos = offset;
                off_t pos_in_file = hdfsTell(fs, fin);
                if (new_pos + (working_buffer.end() - pos) == pos_in_file)
                    return new_pos;

                if (hasPendingData() && new_pos <= pos_in_file && new_pos >= pos_in_file - static_cast<off_t>(working_buffer.size()))
                {
                    /// Position is still inside buffer.
                    pos = working_buffer.begin() + (new_pos - (pos_in_file - working_buffer.size()));
                    return new_pos;
                }
                else
                {
                    pos = working_buffer.end();
                    if (!hdfsSeek(fs, fin, new_pos))
                    {
                        return new_pos;
                    }
                    return -1;
                }
            }
    };
}

#endif
