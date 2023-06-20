/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <random>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/Hive/HiveMetastore.h>
#include <Storages/Hive/TSaslClientTransport.h>
#include <Storages/StorageCnchHive.h>
#include <boost/algorithm/string.hpp>
#include <hivemetastore/hive_metastore_types.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <Poco/DirectoryIterator.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ThreadPool.h>
#include <Common/formatIPv6.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/hex.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>
#include <Access/KerberosInit.h>

#include <iostream>

namespace ProfileEvents
{
extern const Event CatalogTime;
}
namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int NETWORK_ERROR;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int CANNOT_CONVERT_TYPE;
}

//CnchHive partitionID construct rules:
//partition ID is partial of partition path, for example:
//hive partition path : /home/tiger/warehouse/test_tiger.db/testDB/date=20210916/hour=10/
//partition ID        : /date=20210916/hour=10/
//so, getPatitionValues need to parse partitionID.
Strings HiveMetastoreClient::getPartitionValues(
    const StoragePtr & storage, const String & db_name, const String & table_name, const String & table_path, const String & name)
{
    const Strings partitionids = getPartitionIDs(storage, db_name, table_name, table_path);
    Strings res;
    for (const auto & partitionid : partitionids)
    {
        String temp = partitionid;
        if (startsWith(temp, "/"))
        {
            temp = temp.substr(1, temp.size());
        }

        if (endsWith(temp, "/"))
        {
            temp = temp.substr(0, temp.size() - 1);
        }

        std::vector<String> values;
        boost::split(values, temp, boost::is_any_of("/"), boost::token_compress_on);
        for (auto elem : values)
        {
            if (elem.find(name) != String::npos)
            {
                std::vector<String> key_value;
                boost::split(key_value, elem, boost::is_any_of("="), boost::token_compress_on);
                res.push_back(key_value[1]);
            }
        }
    }

    return res;
}

void HiveMetastoreClient::getTable(Table & table, const String & db_name, const String & table_name)
{
    auto client_call = [&](ThriftHiveMetastoreClientPool::Entry & client) { client->get_table(table, db_name, table_name); };
    tryCallHiveClient(client_call);
}

String HiveMetastoreClient::escapeHiveTablePrefix(const String & fullpath, const String & path)
{
    return path.substr(fullpath.size(), path.size());
}

Strings HiveMetastoreClient::getPartitionIDsFromMetastore(
    [[maybe_unused]] const StoragePtr & storage,
    const String & db_name,
    const String & table_name,
    const String & table_path,
    int16_t max_parts)
{
    std::vector<Partition> partitions;
    Strings res;
    auto client_call
        = [&](ThriftHiveMetastoreClientPool::Entry & client) { client->get_partitions(partitions, db_name, table_name, max_parts); };
    tryCallHiveClient(client_call);

    for (const auto & partition : partitions)
    {
        String partitionid = escapeHiveTablePrefix(table_path, normalizeHdfsSchema(partition.sd.location));
        res.push_back(partitionid);
    }

    return res;
}

static Strings getPartsNameInPartition(const DiskPtr & disk, const String & location)
{
    Strings part_names;
    for (auto it = disk->iterateDirectory(location); it->isValid(); it->next())
    {
        if (startsWith(it->name(), "_") || startsWith(it->name(), "."))
            continue;

        part_names.push_back(it->name());
    }

    return part_names;
}

HivePartitionVector HiveMetastoreClient::getPartitionsFromMetastore(
    [[maybe_unused]] const StoragePtr & storage,
    const String & db_name,
    const String & table_name,
    const String & table_path,
    int16_t max_parts)
{
    std::vector<Partition> partitions;
    auto client_call
        = [&](ThriftHiveMetastoreClientPool::Entry & client) { client->get_partitions(partitions, db_name, table_name, max_parts); };
    tryCallHiveClient(client_call);

    if (partitions.empty())
        return {};

    size_t num_threads = 2 * getNumberOfPhysicalCPUCores();
    size_t pool_size = std::min(partitions.size(), num_threads);
    size_t num_per_thread = partitions.size() / pool_size;
    size_t remain = partitions.size() % pool_size;
    HivePartitionVector res(partitions.size());

    ThreadPool thread_pool(pool_size);
    // ExceptionHandler exception_handler;

    // LOG_TRACE(log, "thread pool size: " << pool_size << " num_per_thread: " << num_per_thread << " remain: " << remain);
    auto disk = storage->getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();

    for (size_t cnt = 0, start = 0; cnt < pool_size; ++cnt)
    {
        size_t end = start + num_per_thread + (cnt < remain ? 1 : 0);

        thread_pool.scheduleOrThrow([&, partitions, start, end] {
            for (size_t i = start; i < end; ++i)
            {
                auto info = std::make_shared<HivePartitionInfo>();
                info->partition_path = normalizeHdfsSchema(partitions[i].sd.location);
                info->hdfs_uri = disk->getName();
                info->table_path = table_path;
                info->db_name = partitions[i].dbName;
                info->table_name = partitions[i].tableName;
                info->create_time = partitions[i].createTime;
                info->last_access_time = partitions[i].lastAccessTime;
                info->values = partitions[i].values;
                info->input_format = partitions[i].sd.inputFormat;
                info->cols = partitions[i].sd.cols;
                std::vector<String> parts_name = getPartsNameInPartition(disk, info->partition_path);
                String partition_id = escapeHiveTablePrefix(table_path, info->partition_path);
                info->parts_name = std::move(parts_name);

                res[i] = std::make_shared<HivePartition>(partition_id, *info);
            }
        });

        start = end;
    }

    thread_pool.wait();
    // exception_handler.throwIfException();

    return res;
}

HivePartitionVector HiveMetastoreClient::getPartitionsByFilter(
    [[maybe_unused]] const StoragePtr & storage,
    const String & db_name,
    const String & table_name,
    const String & table_path,
    const String & filter,
    int16_t max_parts)
{
    std::vector<Partition> partitions;
    auto client_call = [&](ThriftHiveMetastoreClientPool::Entry & client) {
        client->get_partitions_by_filter(partitions, db_name, table_name, filter, max_parts);
    };
    tryCallHiveClient(client_call);

    if (partitions.empty())
        return {};

    size_t num_threads = 2 * getNumberOfPhysicalCPUCores();
    size_t pool_size = std::min(partitions.size(), num_threads);
    size_t num_per_thread = partitions.size() / pool_size;
    size_t remain = partitions.size() % pool_size;
    HivePartitionVector res(partitions.size());

    ThreadPool thread_pool(pool_size);
    // ExceptionHandler exception_handler;

    // LOG_TRACE(log, "thread pool size: " << pool_size << " num_per_thread: " << num_per_thread << " remain: " << remain);
    auto disk = storage->getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();

    for (size_t cnt = 0, start = 0; cnt < pool_size; ++cnt)
    {
        size_t end = start + num_per_thread + (cnt < remain ? 1 : 0);

        thread_pool.scheduleOrThrow([&, partitions, start, end] {
            for (size_t i = start; i < end; ++i)
            {
                auto info = std::make_shared<HivePartitionInfo>();
                info->partition_path = normalizeHdfsSchema(partitions[i].sd.location);
                info->table_path = table_path;
                info->db_name = partitions[i].dbName;
                info->table_name = partitions[i].tableName;
                info->hdfs_uri = disk->getName();
                info->create_time = partitions[i].createTime;
                info->last_access_time = partitions[i].lastAccessTime;
                info->values = partitions[i].values;
                info->input_format = partitions[i].sd.inputFormat;
                info->cols = partitions[i].sd.cols;
                std::vector<String> parts_name = getPartsNameInPartition(disk, info->partition_path);
                String partition_id = escapeHiveTablePrefix(table_path, info->partition_path);
                info->parts_name = std::move(parts_name);

                res[i] = std::make_shared<HivePartition>(partition_id, *info);
            }
        });

        start = end;
    }
    thread_pool.wait();
    // exception_handler.throwIfException();

    return res;
}

HivePartitionVector HiveMetastoreClient::getPartitionList(
    const StoragePtr & storage, const String & db_name, const String & table_name, const String & table_path, int16_t max_parts)
{
    //if no cache
    return getPartitionsFromMetastore(storage, db_name, table_name, table_path, max_parts);
}

bool HiveMetastoreClient::getDataPartIndex(const String & part_name, Int64 & index)
{
    if (!startsWith(part_name, "part-"))
        return false;

    /// part-00000-5cf7580f-a3f6-4beb-90a6-e9f4de61c887_00003.c000
    /// 00003 : part index
    std::vector<String> para;
    boost::split(para, part_name, boost::is_any_of("_"), boost::token_compress_on);
    if (para.size() != 2)
        return false;

    /// split_part_name: 00003.c000
    auto split_part_name = para[1];
    para.clear();
    boost::split(para, split_part_name, boost::is_any_of("."), boost::token_compress_on);
    if (para.size() != 2)
        return false;

    index = std::atoi(para[0].c_str());

    return true;
}

HiveDataPartsCNCHVector HiveMetastoreClient::getDataPartsInPartition(
    const StoragePtr & /*storage*/,
    HivePartitionPtr & partition,
    const HDFSConnectionParams &,
    const std::set<Int64> & required_bucket_numbers)
{
    //if no cache
    HiveDataPartsCNCHVector res;
    std::vector<String> parts_name = partition->getPartsName();
    const String partition_id = partition->getID();
    const String table_path = partition->getTablePath();
    std::unordered_set<Int64> skip_list = {};
    const String format_name = partition->getInputFormat();

    for (auto & part_name : parts_name)
    {
        Int64 index = 0;
        if (!required_bucket_numbers.empty() && getDataPartIndex(part_name, index))
        {
            if (required_bucket_numbers.find(index) == required_bucket_numbers.end())
                continue;
        }

        part_name = partition_id + '/' + part_name;
        auto info = std::make_shared<HivePartInfo>(part_name, partition_id);

        LOG_TRACE(&Poco::Logger::get("HiveMetastoreClient"), " getDataPartsInPartition format_name = {}", format_name);

        if (format_name.find("Orc") != String::npos)
            res.push_back(std::make_shared<HiveORCFile>(part_name, partition->getHDFSUri(), table_path, format_name, nullptr, *info, skip_list));
        else if (format_name.find("Parquet") != String::npos)
            res.push_back(std::make_shared<HiveParquetFile>(part_name, partition->getHDFSUri(), table_path, format_name, nullptr, *info, skip_list));
    }

    return res;
}

HiveDataPartsCNCHVector HiveMetastoreClient::getDataPartsInTable(
    const StoragePtr & storage,
    Table & table,
    const HDFSConnectionParams &,
    const std::set<Int64> & required_bucket_numbers)
{
    HiveDataPartsCNCHVector res;

    auto disk = storage->getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
    std::vector<String> parts_name = getPartsNameInPartition(disk, normalizeHdfsSchema(table.sd.location));
    const String table_path = Poco::URI(table.sd.location).getPath();
    std::unordered_set<Int64> skip_list = {};
    const String format_name = table.sd.inputFormat;

    for (auto & part_name : parts_name)
    {
        Int64 index = 0;
        if (!required_bucket_numbers.empty() && getDataPartIndex(part_name, index))
        {
            if (required_bucket_numbers.find(index) == required_bucket_numbers.end())
                continue;
        }

        auto info = std::make_shared<HivePartInfo>(part_name, "");

        LOG_TRACE(&Poco::Logger::get("HiveMetastoreClient"), " getDataPartsInTable format_name = {}", format_name);

        if (format_name.find("Orc") != String::npos)
            res.push_back(std::make_shared<HiveORCFile>(part_name, disk->getName(), table_path, format_name, nullptr, *info, skip_list));
        else if (format_name.find("Parquet") != String::npos)
            res.push_back(std::make_shared<HiveParquetFile>(part_name, disk->getName(), table_path, format_name, nullptr, *info, skip_list));
    }

    return res;
}

String HiveMetastoreClient::normalizeHdfsSchema(const String & path)
{
    Poco::URI uri(path);
    if (uri.getScheme() == "hdfs")
    {
        return uri.getPath();
    }
    else
    {
        // LOG_ERROR(log, "remote hive location path only support hdfs. Currently HDFS schema is: " << uri.getScheme());
        throw Exception("remote hive location path only support hdfs now", ErrorCodes::LOGICAL_ERROR);
    }
}

Strings HiveMetastoreClient::getPartitionIDs(
    const StoragePtr & storage, const String & db_name, const String & table_name, const String & table_path)
{
    //if no cache
    return getPartitionIDsFromMetastore(storage, db_name, table_name, table_path);
}

void HiveMetastoreClient::getColumns(std::vector<FieldSchema> & result, const String & db_name, const String & table_name)
{
    auto client_call = [&](ThriftHiveMetastoreClientPool::Entry & client) { client->get_schema(result, db_name, table_name); };
    tryCallHiveClient(client_call);
}

void HiveMetastoreClient::tryCallHiveClient(std::function<void(ThriftHiveMetastoreClientPool::Entry &)> func)
{
    size_t i = 0;
    String err_msg;

    for (; i < settings.max_hive_metastore_client_retry; ++i)
    {
        auto client = client_pool.get(settings.get_hive_metastore_client_timeout);
        try
        {
            func(client);
        }
        catch (apache::thrift::transport::TTransportException & e)
        {
            // client.expire();
            err_msg = e.what();
            continue;
        }
        catch (...)
        {
            // err_msg = e.what;
            LOG_TRACE(&Poco::Logger::get("HiveMetastoreClient"), "try call hive metastore occur unexcepted excetion. ");
            continue;
        }
        break;
    }

    if (i > settings.max_hive_metastore_client_retry)
        throw Exception("Hive Metastore expired because " + err_msg, ErrorCodes::NETWORK_ERROR);
}

HiveMetastoreClientFactory & HiveMetastoreClientFactory::instance()
{
    static HiveMetastoreClientFactory factory;
    return factory;
}

HiveMetastoreClientPtr HiveMetastoreClientFactory::getOrCreate(const String & name, const CnchHiveSettings & settings)
{
    std::lock_guard lock(mutex);
    auto it = clients.find(name);
    if (it == clients.end())
    {
        auto builder = [name, settings]() { return createThriftHiveMetastoreClient(name, settings); };

        auto client = std::make_shared<HiveMetastoreClient>(builder, settings);
        clients.emplace(name, client);

        LOG_TRACE(&Poco::Logger::get("HiveMetastoreClientFactory"), "insert getOrCreate, hms psm {}", name);

        return client;
    }

    LOG_TRACE(&Poco::Logger::get("HiveMetastoreClientFactory"), "getOrCreate, hms psm {}", name);

    return it->second;
}

std::shared_ptr<ThriftHiveMetastoreClient>
HiveMetastoreClientFactory::createThriftHiveMetastoreClient(const String & name, const CnchHiveSettings & settings)
{
    LOG_TRACE(&Poco::Logger::get("HiveMetastoreClientFactory"), "CnchHive createThriftHiveMetastoreClient hivemetastore psm: {}", name);
    /// connect to hive metastore
    Poco::URI hms_url(name);
    auto host = hms_url.getHost();
    auto port = hms_url.getPort();

    if (host.empty() || port == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "host empty or port is 0. host {}, port {}", host, port);

    LOG_TRACE(&Poco::Logger::get("HiveMetastoreClientFactory"), "CnchHive connect HiveMetastore host: {} {}", host, port);
    std::shared_ptr<TSocket> socket = std::make_shared<TSocket>(host, port);
    socket->setKeepAlive(true);
    socket->setConnTimeout(settings.hive_metastore_client_conn_timeout);
    socket->setRecvTimeout(settings.hive_metastore_client_recv_timeout);
    socket->setSendTimeout(settings.hive_metastore_client_send_timeout);
    std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));

    if (settings.hive_metastore_client_kerberos_auth)
    {
        String hadoop_kerberos_principal = settings.hive_metastore_client_principal.toString() + "/"+ settings.hive_metastore_client_service_fqdn.toString();
        kerberosInit(settings.hive_metastore_client_keytab_path,hadoop_kerberos_principal);
        transport = TSaslClientTransport::wrapClientTransports(settings.hive_metastore_client_service_fqdn, settings.hive_metastore_client_principal, transport);
    }

    std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    std::shared_ptr<ThriftHiveMetastoreClient> client = std::make_shared<ThriftHiveMetastoreClient>(protocol);

    try
    {
        transport->open();
    }
    catch (TException & tx)
    {
        throw Exception(" connect to hive metastore: " + name + " failed." + tx.what(), ErrorCodes::BAD_ARGUMENTS);
    }
    catch (...)
    {
        throw;
    }

    LOG_TRACE(&Poco::Logger::get("HiveMetastoreClientFactory"), "Finish CnchHive connect HiveMetastore host");
    return client;
}

ThriftHiveMetastoreClientPool::ThriftHiveMetastoreClientPool(
    ThriftHiveMetastoreClientBuilder builder_, int max_hive_metastore_client_connections)
    : PoolBase<Object>(max_hive_metastore_client_connections, &Poco::Logger::get("ThriftHiveMetastoreClientPool")), builder(builder_)
{
}

}
