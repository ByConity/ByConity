#include "ILakeScanInfo.h"

#include <Protos/lake_models.pb.h>
#include <Storages/DataLakes/ScanInfo/FileScanInfo.h>
#include <Storages/DataLakes/ScanInfo/HudiJNIScanInfo.h>
#include <Storages/DataLakes/ScanInfo/PaimonJNIScanInfo.h>
#include <Poco/DigestStream.h>
#include <Poco/HexBinaryEncoder.h>
#include <Poco/MD5Engine.h>
#include <Common/ConsistentHashUtils/Hash.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PROTOBUF_FORMAT;
}

ILakeScanInfo::ILakeScanInfo(StorageType storage_type_) : storage_type(storage_type_)
{
}

size_t ILakeScanInfo::calWorkerIdx(size_t worker_num)
{
    if (distribution_id.has_value())
        return distribution_id.value() % worker_num;
    else
        return consistentHashForString(identifier(), worker_num);
}

void ILakeScanInfo::serialize(Protos::LakeScanInfo & proto) const
{
    proto.set_storage_type(storage_type);
}

LakeScanInfoPtr ILakeScanInfo::deserialize(
    const Protos::LakeScanInfo & proto, const ContextPtr & context, const StorageMetadataPtr & metadata, const CnchHiveSettings & settings)
{
    if (proto.has_file_scan_info())
        return FileScanInfo::deserialize(proto, context, metadata, settings);
#if USE_JAVA_EXTENSIONS
    else if (proto.has_paimon_jni_scan_info())
        return PaimonJNIScanInfo::deserialize(proto, context, metadata, settings);
    else if (proto.has_hudi_jni_scan_info())
        return HudiJNIScanInfo::deserialize(proto, context, metadata, settings);
#endif

    throw Exception("Unknown protobuf format", ErrorCodes::UNKNOWN_PROTOBUF_FORMAT);
}

LakeScanInfos ILakeScanInfo::deserialize(
    const Protos::LakeScanInfos & proto, const ContextPtr & context, const StorageMetadataPtr & metadata, const CnchHiveSettings & settings)
{
    LakeScanInfos scan_infos;
    for (const auto & scan_info_proto : proto.scan_infos())
    {
        scan_infos.emplace_back(deserialize(scan_info_proto, context, metadata, settings));
    }
    return scan_infos;
}

String ILakeScanInfo::md5(const String & content)
{
    Poco::MD5Engine md5_engine;
    Poco::DigestOutputStream out(md5_engine);
    out << content;
    out.close();
    return Poco::DigestEngine::digestToHex(md5_engine.digest());
}
}
