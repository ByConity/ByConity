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

#pragma once

#include <Core/Defines.h>
#include <Core/Types.h>
#include <Poco/Util/AbstractConfiguration.h>
// #include <Core/SettingsCommon.h>
#include <Core/BaseSettings.h>
#include <Common/SettingsChanges.h>

namespace DB
{
class ASTStorage;
struct Settings;

/** Settings for the CnchHive family of engines.
  * Could be loaded from config or from a CREATE TABLE query (SETTINGS clause).
  */

#define APPLY_FOR_CNCHHIVE_SETTINGS(M) \
    /** How many rows correspond to one primary key value. */ \
    M(UInt64, index_granularity, 8192, "", 0) \
    M(String, cnch_server_vw, DEFAULT_SERVER_VW_NAME, "server vw", 0) \
    M(String, cnch_vw_default, "vw_default", "read vw", 0) \
    M(String, cnch_vw_read, "vw_read", "deprecated read settings, use cnch_vw_default instead", 0) \
    M(String, cnch_vw_write, "vw_write", "Not used for hive", 0) \
    M(Bool, enable_schema_covert_fault_tolerance, true, "Whether simply ignore error when column type of lake talbe is not supported by native.", 0) \
    /** Parquet skip useless row group */ \
    M(Bool, enable_skip_row_group, false, "", 0) \
    /** allocate part policy**/ \
    M(Bool, use_simple_hash, true, "", 0) \
    /** parallel read parquet max threads **/ \
    M(UInt64, max_read_row_group_threads, 32, "", 0) \
    M(Bool, cnch_temporary_table, 0, "", 0) \
    M(Bool, enable_local_disk_cache, false, "", 0)  \
    M(String, hive_metastore_url, "", "Hive metastore url", 0) \
    M(String, hdfs_fs, "", "Hdfs namenode url", 0) \
    M(String, endpoint, "", "S3 endpoint", 0) \
    M(String, ak_id, "", "S3 access key", 0) \
    M(String, ak_secret, "", "S3 secret key", 0) \
    M(String, region, "", "S3 region", 0) \
    M(Bool, merge_partition_stats, false, "merge all partition stats", 0) \
    M(Bool, s3_use_virtual_hosted_style, true, "use virtual hosted style", 0) \
    /** HMS kerberos settings **/ \
    M(Bool, hive_metastore_client_kerberos_auth, 0, "Enable hms auth with Kerberos", 0) \
    M(String, hive_metastore_client_service_fqdn, "", "The fqdn for auth server", 0) \
    M(String, hive_metastore_client_keytab_path, "/etc/krb5.keytab", "The path of Kerberos keytab for hms auth", 0) \
    M(String, hive_metastore_client_principal, "hive", "The Kerberos principal for hms auth", 0) \
    /** S3 settings **/ \
    M(String, s3_extra_options, "", "S3 extra options, delimited by ','", 0) \
    M(String, meta_type, "hive", "hive for hive, lf for lasformation, glue for aws glue", 0) \
    /** las formation settings **/ \
    M(String, lf_metastore_url, "thrift://dummy:9083", "lasformation thrift addr", 0) \
    M(String, lf_metastore_region, "cn-beijing", "region", 0) \
    M(String, lf_metastore_catalog, "hive", "the catalog to be mapped", 0) \
    M(String, lf_metastore_ak_id, "dummy-ak", "lf ak", 0) \
    M(String, lf_metastore_ak_secret, "dummy-sk", "lf sk", 0) \
    M(String, aws_glue_catalog_id, "aws.glue.catalog_id", "aws.glue.catalog_id", 0) \
    M(String, aws_glue_ak_id, "aws.glue.access_key", "aws.glue.access_key", 0) \
    M(String, aws_glue_ak_secret, "aws.glue.secret_key", "aws.glue.secret_key", 0) \
    M(String, aws_glue_region, "aws.glue.region", "aws.glue.region", 0) \
    M(String, aws_glue_endpoint, "aws.glue.endpoint", "aws.glue.endpoint", 0) \
    M(Bool, aws_glue_use_instance_profile, false, "aws.glue.use_instance_profile", 0) \


/// Settings that should not change after the creation of a table.
#define APPLY_FOR_IMMUTABLE_CNCH_HIVE_SETTINGS(M) M(index_granularity)

DECLARE_SETTINGS_TRAITS(CnchHiveSettingsTraits, APPLY_FOR_CNCHHIVE_SETTINGS)


struct CnchHiveSettings : public BaseSettings<CnchHiveSettingsTraits>
{
public:
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    /// NOTE: will rewrite the AST to add immutable settings.
    void loadFromQuery(ASTStorage & storage_def);

    static std::vector<String> splitStr(const String & str, const String & delimiters);
};

using CnchHiveSettingsPtr = std::shared_ptr<CnchHiveSettings>;

}
