#pragma once

#include <Common/config.h>

#if USE_LASFS

#    include <map>
#    include <string>
#    include <IO/LasfsCommon.h>
#    include <IO/WriteBufferFromFileBase.h>
#    include <Lasfs/fs.h>

namespace DB
{
class WriteBufferFromLasfs : public WriteBufferFromFileBase
{
private:
    std::map<std::string, std::string> settings;
    las::lasfsBuilder * builder;
    las::lasFS fs;
    las::lasFile file;

    std::string filepath;
    int flags;

    int buffer_size;

    //Create and Set configuration for the lasfsBuilder
    //set las::lasfsBuilder* builder;
    void createAndSetLasfsBuilder();

    //lasfsBuilderConnect lasfsOpenFile
    //set las::lasFS fs and las::LasFile file;
    void openConnectAndCreateFile();

    //lasfsCloseFile free lasfsDisconnect
    void closeFileAndDisconnect();

public:
    // Construct WriteBufferFromLasfs from a copy of LasfsSettings
    // Build lasfs builder and Set configuration string for an lasfsBuilder
    // Then create and open lasfile
    explicit WriteBufferFromLasfs(
        std::map<std::string, std::string> settings,
        const std::string & filepath,
        size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = 777);


    //write buf to file if !offset()
    void nextImpl() override;

    //flush the buffer and  close File  then disconnect
    /// FIXME: this is a non-standard behavior (e.g., can't write after sync)
    void sync() override { finalize(); }

    //get file path
    std::string getFileName() const override;

    //do sync() if file is open
    ~WriteBufferFromLasfs() override;

    WriteBuffer * inplaceReconstruct(const String & out_path, [[maybe_unused]] std::unique_ptr<WriteBuffer> nested) override
    {
        std::map<std::string, std::string> settings_tmp = std::move(this->settings);
        const Poco::URI out_uri(out_path);
        // Call the destructor explicitly but does not free memory
        this->~WriteBufferFromLasfs();
        new (this) WriteBufferFromLasfs(settings_tmp, "lasfs:/" + out_uri.getHost() + out_uri.getPath());
        return this;
    }
protected:
    void finalizeImpl() override;
};


}

#endif
