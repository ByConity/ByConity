#pragma once

#include <Interpreters/Context.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>
#include <BridgeHelper/LibraryBridgeHelper.h>


namespace DB
{

class Pipe;

// Class to access the external dictionary part of the clickhouse-library-bridge.
class ExternalDictionaryLibraryBridgeHelper final : public LibraryBridgeHelper
{

public:
    struct LibraryInitData
    {
        String library_path;
        String library_settings;
        String dict_attributes;
    };

    static constexpr inline auto PING_HANDLER = "/extdict_ping";
    static constexpr inline auto MAIN_HANDLER = "/extdict_request";

    ExternalDictionaryLibraryBridgeHelper(ContextPtr context_, const Block & sample_block, const Field & dictionary_id_, const LibraryInitData & library_data_);

    bool initLibrary();

    bool cloneLibrary(const Field & other_dictionary_id);

    bool removeLibrary();

    bool isModified();

    bool supportsSelectiveLoad();

    BlockInputStreamPtr loadAll();

    BlockInputStreamPtr loadIds(const std::vector<uint64_t> & ids);

    BlockInputStreamPtr loadKeys(const Block & requested_block);

    LibraryInitData getLibraryData() const { return library_data; }

protected:
    Poco::URI getPingURI() const override;

    Poco::URI getMainURI() const override;

    bool bridgeHandShake() override;

    BlockInputStreamPtr loadBase(const Poco::URI & uri, ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = {});

    bool executeRequest(const Poco::URI & uri, ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = {}) const;

    ReadWriteBufferFromHTTP::OutStreamCallback getInitLibraryCallback() const;

private:
    static constexpr inline auto EXT_DICT_LIB_NEW_METHOD = "extDict_libNew";
    static constexpr inline auto EXT_DICT_LIB_CLONE_METHOD = "extDict_libClone";
    static constexpr inline auto EXT_DICT_LIB_DELETE_METHOD = "extDict_libDelete";
    static constexpr inline auto EXT_DICT_LOAD_ALL_METHOD = "extDict_loadAll";
    static constexpr inline auto EXT_DICT_LOAD_IDS_METHOD = "extDict_loadIds";
    static constexpr inline auto EXT_DICT_LOAD_KEYS_METHOD = "extDict_loadKeys";
    static constexpr inline auto EXT_DICT_IS_MODIFIED_METHOD = "extDict_isModified";
    static constexpr inline auto EXT_DICT_SUPPORTS_SELECTIVE_LOAD_METHOD = "extDict_supportsSelectiveLoad";

    Poco::URI createRequestURI(const String & method) const;

    const Block sample_block;
    LibraryInitData library_data;
    Field dictionary_id;
    bool library_initialized = false;
};

}
