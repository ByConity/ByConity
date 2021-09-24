#include <Common/KMSClient.h>

#include <Common/Exception.h>
#include <Columns/ColumnNullable.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Poco/String.h>
#include <Poco/MD5Engine.h>
#include <Poco/DigestStream.h>
#include <Interpreters/Context.h>

#include <boost/algorithm/string.hpp>
#include <common/logger_useful.h>
#include <common/DateLUT.h>

#include <random>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


void KMSClient::initKMSClient(const std::string & psm, const std::string & name, const std::string & cluster, const std::string & shared)
{
    ch_cluster = cluster;
    log = &Poco::Logger::get("KMSClient");
    client = std::make_shared<security::kms::KMSClient>(psm);
    cipher = client->DecryptDataKeyByPSMAndName(psm, name);

    if (!shared.empty())
        boost::split(shared_clusters, shared, boost::is_any_of(" ,"), boost::token_compress_on);

    if (cipher)
        LOG_DEBUG(log, "Successful start kms-server on psm " + psm);
    else
        LOG_WARNING(log, "Failed start kms-server on psm " + psm);
}

std::string KMSClient::encrypt(const std::string & data) const
{
    if (data.empty())
        return {};

    /// EncryptForQuery: Fixed encryption
    /// Encrypt: Random encryption

    auto encrypted = cipher->EncryptForQuery(data);

    if (!encrypted)
        throw Exception("Failed encrypt data " + data, ErrorCodes::LOGICAL_ERROR);

    return encrypted.value();
}

std::string KMSClient::decrypt(const std::string & data) const
{
    if (data.empty())
        return {};

    auto decrypted = cipher->Decrypt(data);

    if (!decrypted)
        throw Exception("Failed decrypt data.", ErrorCodes::LOGICAL_ERROR);

    return decrypted.value();
}

void KMSClient::createKmsConfig(const std::string & config_name) const
{
    std::random_device rd;
    auto gen = std::mt19937(rd()); // mt19937 engine
    std::uniform_int_distribution<int> dis(1, std::numeric_limits<int>::max());

    std::string key = config_name + std::to_string(time(nullptr)) + std::to_string(dis(gen));

    Poco::MD5Engine md5;
    Poco::DigestOutputStream out(md5);
    out << key;
    out.flush();
    createKmsConfig(config_name, Poco::DigestEngine::digestToHex(md5.digest()));
}

static String toString(const std::vector<String> & names)
{
    String res;
    for (const auto & name : names)
        res += name;
    return res;
}

void KMSClient::createKmsConfig(const std::string & config_name, const std::string & key) const
{
    const std::string kms_config_name = ch_cluster + "." + config_name;
    auto config = tryGetConfig(kms_config_name);

    if (config)
    {
        if (!config->Result.empty())
            LOG_TRACE(log, "KMS Config " + kms_config_name + " already exists. Skip this shard");
        else
            client->EditConfig(kms_config_name, config->Cluster, key);
        return;
    }

    client->NewConfigAndShare(kms_config_name, "cn", key, shared_clusters);
    LOG_DEBUG(log, "Create kms config '" + kms_config_name + "' and shared to clusters: " + toString(shared_clusters));
}

bool KMSClient::auth(const std::string & config_name, const String & key, const ContextPtr & global_context) const
{
    if (key.empty())
        return false;

    NameSet split_keys;
    String lower_key = Poco::toLower(key);
    boost::split(split_keys, lower_key , boost::is_any_of(" ,"), boost::token_compress_on);

    return auth(config_name, split_keys, global_context);
}

bool KMSClient::auth(const std::string & config_name, const NameSet & keys, const ContextPtr & global_context) const
{
    auto kms_config_name = ch_cluster + "." + config_name;
    auto kms_key = global_context->getKMSKeyCache(kms_config_name);

    if (kms_key.empty())
    {
        auto res = tryGetConfig(kms_config_name);

        if (res)
        {
            /// and Cache to Context
            global_context->addKMSKeyCache(kms_config_name, res->Result);
            kms_key = res->Result;
        }
    }

    if (kms_key.empty())
        return false;

    /// check encrypt_key format "md5(date + '_' + kmsToken)"
    auto check_encrypt_key = [&](time_t now_time) -> bool
    {
        const DateLUTImpl & date_lut = DateLUT::instance("UTC");
        String token = date_lut.dateToString(now_time) + "_" + kms_key;
        Poco::MD5Engine md5;
        Poco::DigestOutputStream md5str(md5);
        md5str << token;
        md5str.flush();
        return keys.count(Poco::DigestEngine::digestToHex(md5.digest()));
    };

    time_t now = time(nullptr);
    return check_encrypt_key(now) || check_encrypt_key(now - 86400);
}

void KMSClient::deleteKmsConfig(const std::string & config_name) const
{
    auto kms_config_name = ch_cluster + "." + config_name;
    auto config = tryGetConfig(kms_config_name);

    if (config)
    {
        if (config->Result.empty())
        {
            LOG_TRACE(log, "KMS Config " + kms_config_name + " already delete, skip this shared");
            return;
        }
        client->EditConfig(kms_config_name, config->Cluster, "");
    }

    LOG_DEBUG(log, "Delete kms config '" + kms_config_name + "'");
}

KMSClient::ConfigResultPtr KMSClient::tryGetConfig(const DB::String & config_name) const
{
    try
    {
        auto config = client->GetConfigByName(config_name);
        if (config)
            return config;
    }
    catch (...)
    {
        LOG_ERROR(log, "Error occurs when get KMS Config " + config_name + "");
    }

    return nullptr;
}

ColumnPtr KMSClient::encryptColumn(const ColumnWithTypeAndName & input_column, bool dry_run)
{
    if (dry_run)
        return input_column.column;

    if (!input_column.type)
        throw Exception("Known type in encryptColumn: " + input_column.name, ErrorCodes::LOGICAL_ERROR);

    LOG_DEBUG(&Poco::Logger::get("EncryptColumn"), "Prepare encrypt column: " + input_column.name);

    auto & kms_client = KMSClient::instance();
    auto encrypt_column = input_column.type->createColumn();

    for (size_t i = 0, size = input_column.column->size(); i < size; ++i)
    {
        if (const auto * null_column = dynamic_cast<const ColumnNullable *>(input_column.column.get()))
        {
            if (!null_column->isNullAt(i))
            {
                auto data = null_column->getNestedColumn().getDataAt(i);
                encrypt_column->insert(kms_client.encrypt(data.toString()));
            }
            else
                encrypt_column->insertDefault();
        }
        else
        {
            auto data = input_column.column->getDataAt(i);
            encrypt_column->insert(kms_client.encrypt(data.toString()));
        }
    }

    return encrypt_column;
}

ColumnPtr KMSClient::decryptColumn(const ColumnWithTypeAndName & input_column, bool dry_run)
{
    if (dry_run)
        return input_column.column;

    if (!input_column.type)
        throw Exception("Known type in encryptColumn: " + input_column.name, ErrorCodes::LOGICAL_ERROR);

    LOG_DEBUG(&Poco::Logger::get("DecryptColumn"), "Prepare decrypt column: " + input_column.name);

    auto & kms_client = KMSClient::instance();
    auto decrypted_column = input_column.type->createColumn();

    for (size_t i = 0, size = input_column.column->size(); i < size; ++i)
    {
        if (const auto * null_column = dynamic_cast<const ColumnNullable *>(input_column.column.get()))
        {
            if (!null_column->isNullAt(i))
            {
                auto data = null_column->getNestedColumn().getDataAt(i);
                decrypted_column->insert(kms_client.decrypt(data.toString()));
            }
            else
                decrypted_column->insertDefault();
        }
        else
        {
            auto data = input_column.column->getDataAt(i);
            decrypted_column->insert(kms_client.decrypt(data.toString()));
        }
    }

    return decrypted_column;
}

}
