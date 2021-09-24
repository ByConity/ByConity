#pragma once
#include <memory>
#include <string>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Poco/Logger.h>
#include <kms/KMSClient.h>


namespace DB
{

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class KMSClient : private boost::noncopyable
{
    using SecurityKMSClientPtr = std::shared_ptr<security::kms::KMSClient>;
    using SecurityAecCipherPtr = std::shared_ptr<security::kms::AesCipher>;
    using ConfigResultPtr = std::shared_ptr<security::kms::ConfigResult>;

public:
    static ALWAYS_INLINE KMSClient & instance(bool check = true)
    {
        static KMSClient client;

        if (check && !client.cipher)
            throw Exception("KMS Cipher initialization failed.", ErrorCodes::LOGICAL_ERROR);

        return client;
    }

    static void init(const std::string & psm, const std::string & name, const std::string & cluster, const std::string & shared)
    {
        auto & client = instance(false);
        client.initKMSClient(psm, name, cluster, shared);
    }

    std::string encrypt(const std::string & data) const;
    std::string decrypt(const std::string & data) const;

    // upload config to kms
    void createKmsConfig(const std::string & config_name) const;
    void deleteKmsConfig(const std::string & config_name) const;

    bool auth(const std::string & config_name, const std::string & key, const ContextPtr & global_context) const;
    bool auth(const std::string & config_name, const NameSet & keys, const ContextPtr & global_context) const;

    static ColumnPtr encryptColumn(const ColumnWithTypeAndName & input_column, bool dry_run);
    static ColumnPtr decryptColumn(const ColumnWithTypeAndName & input_column, bool dry_run);

private:
    KMSClient() = default;

    ConfigResultPtr tryGetConfig(const std::string & config_name) const;
    void initKMSClient(const std::string & psm, const std::string & name, const std::string & cluster, const std::string & shared);
    void createKmsConfig(const std::string & config_name, const std::string & token) const;

    Poco::Logger * log;
    std::string ch_cluster;
    SecurityKMSClientPtr client;
    SecurityAecCipherPtr cipher;
    std::vector<std::string> shared_clusters;
};

}
