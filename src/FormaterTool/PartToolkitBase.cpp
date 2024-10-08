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

#include <FormaterTool/PartToolkitBase.h>
#include <FormaterTool/ZipHelper.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTPartToolKit.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMergeTree.h>
#include <Poco/JSON/Template.h>
#include <DataTypes/DataTypeBitMap64.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

const std::string default_config = "<yandex>\n"
                                   "<storage_configuration>\n"
                                   "<disks>\n"
                                   "    <hdfs>\n"
                                   "        <path><?= source-path ?></path>\n"
                                   "        <type>hdfs</type>\n"
                                   "    </hdfs>\n"
                                   "</disks>\n"
                                   "<policies>\n"
                                   "    <cnch_default_hdfs>\n"
                                   "        <volumes>\n"
                                   "            <hdfs>\n"
                                   "                <default>hdfs</default>\n"
                                   "                <disk>hdfs</disk>\n"
                                   "            </hdfs>\n"
                                   "        </volumes>\n"
                                   "    </cnch_default_hdfs>\n"
                                   "</policies>\n"
                                   "</storage_configuration>\n"
                                   "</yandex>";

PartToolkitBase::PartToolkitBase(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}

PartToolkitBase::~PartToolkitBase()
{
}

void PartToolkitBase::applySettings()
{
    auto & mergetree_settings = const_cast<MergeTreeSettings &>(getContext()->getMergeTreeSettings());
    auto & settings = const_cast<Settings &>(getContext()->getSettingsRef());

    /*** apply some default settings ***/
    mergetree_settings.set("enable_metastore", false);
    // pw specific settings
    mergetree_settings.set("min_rows_for_compact_part", 0);
    mergetree_settings.set("min_bytes_for_compact_part", 0);
    mergetree_settings.set("enable_local_disk_cache",0);
    mergetree_settings.set("enable_nexus_fs", 0);
    settings.set("input_format_skip_unknown_fields", true);
    settings.set("skip_nullinput_notnull_col", true);


    /// apply user defined settings.
    const ASTPartToolKit & pw_query = query_ptr->as<ASTPartToolKit &>();
    if (pw_query.settings)
    {
        const ASTSetQuery & set_ast = pw_query.settings->as<ASTSetQuery &>();
        for (const auto & change : set_ast.changes)
        {
            if (settings.has(change.name))
                settings.set(change.name, change.value);
            else if (mergetree_settings.has(change.name))
                mergetree_settings.set(change.name, change.value);
            else if (change.name == "hdfs_user")
                getContext()->setHdfsUser(change.value.safeGet<String>());
            else if (change.name == "hdfs_nnproxy")
                getContext()->setHdfsNNProxy(change.value.safeGet<String>());
            else if (change.name == "s3_input_config") {
                s3_input_config = std::make_unique<S3::S3Config>(change.value.safeGet<String>());
            } else if (change.name == "s3_output_config") {
                s3_output_config = std::make_unique<S3::S3Config>(change.value.safeGet<String>());
            }
            else
                user_settings.emplace(change.name, change.value);
        }
    }
    if (pw_query.s3_clean_task_info) {
        /// There is no need to initialize further in `S3 CLEAN` mode.
        return;
    }

    /// Init HDFS params.
    ///
    /// User can bypass nnproxy by passing a string to `hdfs_nnproxy` with prefixs like `hdfs://` or `cfs://`.
    HDFSConnectionParams hdfs_params
        = HDFSConnectionParams::parseFromMisusedNNProxyStr(getContext()->getHdfsNNProxy(), getContext()->getHdfsUser());
    getContext()->setHdfsConnectionParams(hdfs_params);

    /// Register default HDFS file system as well in case of
    /// lower level logic call `getDefaultHdfsFileSystem`.
    /// Default values are the same as those on the ClickHouse server.
    {
        const int hdfs_max_fd_num = user_settings.count("hdfs_max_fd_num") ? user_settings["hdfs_max_fd_num"].safeGet<int>() : 100000;
        const int hdfs_skip_fd_num = user_settings.count("hdfs_skip_fd_num") ? user_settings["hdfs_skip_fd_num"].safeGet<int>() : 100;
        const int hdfs_io_error_num_to_reconnect
            = user_settings.count("hdfs_io_error_num_to_reconnect") ? user_settings["hdfs_io_error_num_to_reconnect"].safeGet<int>() : 10;
        registerDefaultHdfsFileSystem(hdfs_params, hdfs_max_fd_num, hdfs_skip_fd_num, hdfs_io_error_num_to_reconnect);
    }

    /// Renders default config to initialize storage configurations.
    Poco::JSON::Object::Ptr params = new Poco::JSON::Object();
    params->set("source-path", pw_query.source_path->as<ASTLiteral &>().value.safeGet<String>());
    Poco::JSON::Template tpl;
    tpl.parse(default_config);
    std::stringstream out;
    tpl.render(params, out);
    std::string default_xml_config = "<?xml version=\"1.0\"?>";
    default_xml_config = default_xml_config + out.str();

    DB::ConfigProcessor config_processor("", false, false);
    auto config = config_processor.loadConfig(default_xml_config).configuration;
    getContext()->setConfig(config);

    /// Overwrite settings if `s3_input_config` is provided.
    if (s3_input_config) {
        settings.set("s3_endpoint", s3_input_config->endpoint);
        settings.set("s3_ak_id", s3_input_config->ak_id);
        settings.set("s3_ak_secret", s3_input_config->ak_secret);
        settings.set("s3_region", s3_input_config->region);
    }
}

StoragePtr PartToolkitBase::getTable()
{
    if (storage)
        return storage;
    else
    {
        const ASTPartToolKit & pw_query = query_ptr->as<ASTPartToolKit &>();
        ASTPtr create_query = pw_query.create_query;
        auto & create = create_query->as<ASTCreateQuery &>();

        if (!create.storage || !create.columns_list)
            throw Exception("Wrong create query.", ErrorCodes::INCORRECT_QUERY);

        if (create.storage->engine != nullptr && create.storage->engine->name != "CloudMergeTree")
            throw Exception("Only support CloudMergeTree in Part Tool.", ErrorCodes::INCORRECT_QUERY);

        ColumnsDescription columns = InterpreterCreateQuery::getColumnsDescription(*create.columns_list->columns, getContext(), create.attach, create.database == "system");
        ConstraintsDescription constraints = InterpreterCreateQuery::getConstraintsDescription(create.columns_list->constraints);
        ForeignKeysDescription foreign_keys = InterpreterCreateQuery::getForeignKeysDescription(create.columns_list->foreign_keys);
        UniqueNotEnforcedDescription unique = InterpreterCreateQuery::getUniqueNotEnforcedDescription(create.columns_list->unique);

        /// In PartTools, BitEngineEncode is illegal, discard
        processIgnoreBitEngineEncode(columns);

        StoragePtr res = StorageFactory::instance().get(
            create,
            PT_RELATIVE_LOCAL_PATH,
            getContext(),
            getContext()->getGlobalContext(),
            columns,
            constraints,
            foreign_keys,
            unique,
            false);

        storage = res;
        return res;
    }
}

void PartToolkitBase::processIgnoreBitEngineEncode(ColumnsDescription & columns)
{
    auto reset_bitengine_encode = [](auto & column)
    {
        if (column.type->isBitEngineEncode())
        {
            auto bitmap_type = std::make_shared<DataTypeBitMap64>();
            bitmap_type->setFlags(column.type->getFlags());
            bitmap_type->resetFlags(TYPE_BITENGINE_ENCODE_FLAG);
            const_cast<ColumnDescription &>(column).type = std::move(bitmap_type);
        }
    };
    std::for_each(columns.begin(), columns.end(), reset_bitengine_encode);
}


PartNamesWithDisks PartToolkitBase::collectPartsFromSource(const String & source_dirs_str, const String & dest_dir)
{
    std::vector<String> source_dirs;
    /// parse all source directories from input path string;
    size_t begin_pos = 0;
    size_t len = 0;
    auto pos = source_dirs_str.find(",", begin_pos);
    while (pos != source_dirs_str.npos)
    {
        len = pos - begin_pos;
        if (len > 0)
        {
            source_dirs.emplace_back(source_dirs_str.substr(begin_pos, len));
        }
        begin_pos = pos + 1;
        pos = source_dirs_str.find(",", begin_pos);
    }
    if (begin_pos < source_dirs_str.size())
    {
        source_dirs.emplace_back(source_dirs_str.substr(begin_pos));
    }

    if (!fs::exists(dest_dir))
        fs::create_directories(dest_dir);

    PartNamesWithDisks res;
    auto disk_ptr = getContext()->getDisk("default");
    ZipHelper ziphelper;

    Poco::DirectoryIterator dir_end;
    for (auto & dir : source_dirs)
    {
        for (Poco::DirectoryIterator it(dir); it != dir_end; ++it)
        {
            const String & file_name = it.name();
            if (it->isFile() && endsWith(file_name, ".zip"))
            {
                String part_name = file_name.substr(0, file_name.find(".zip"));
                fs::rename(dir + file_name, dest_dir + file_name);
                ziphelper.unzipFile(dest_dir + file_name, dest_dir + part_name);
                res.emplace_back(part_name, disk_ptr);
            }
        }
    }

    return res;
}

}
