#include <Storages/System/StorageSystemCnchUserPriv.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Access/AccessControlManager.h>
#include <Access/EnabledRoles.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/User.h>
#include <Access/AccessFlags.h>
#include <Common/StringUtils/StringUtils.h>
#include <sstream>


namespace DB
{

NamesAndTypesList StorageSystemCnchUserPriv::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"host", std::make_shared<DataTypeString>()},
        {"user", std::make_shared<DataTypeString>()},
        {"select_priv", std::make_shared<DataTypeString>()},
        {"insert_priv", std::make_shared<DataTypeString>()},
        {"update_priv", std::make_shared<DataTypeString>()},
        {"delete_priv", std::make_shared<DataTypeString>()},
        {"create_priv", std::make_shared<DataTypeString>()},
        {"drop_priv", std::make_shared<DataTypeString>()},
        {"reload_priv", std::make_shared<DataTypeString>()},
        {"shutdown_priv", std::make_shared<DataTypeString>()},
        {"process_priv", std::make_shared<DataTypeString>()},
        {"file_priv", std::make_shared<DataTypeString>()},
        {"grant_priv", std::make_shared<DataTypeString>()},
        {"references_priv", std::make_shared<DataTypeString>()},
        {"index_priv", std::make_shared<DataTypeString>()},
        {"alter_priv", std::make_shared<DataTypeString>()},
        {"show_db_priv", std::make_shared<DataTypeString>()},
        {"super_priv", std::make_shared<DataTypeString>()},
        {"create_tmp_table_priv", std::make_shared<DataTypeString>()},
        {"lock_tables_priv", std::make_shared<DataTypeString>()},
        {"execute_priv", std::make_shared<DataTypeString>()},
        {"repl_slave_priv", std::make_shared<DataTypeString>()},
        {"repl_client_priv", std::make_shared<DataTypeString>()},
        {"create_view_priv", std::make_shared<DataTypeString>()},
        {"show_view_priv", std::make_shared<DataTypeString>()},
        {"create_routine_priv", std::make_shared<DataTypeString>()},
        {"alter_routine_priv", std::make_shared<DataTypeString>()},
        {"create_user_priv", std::make_shared<DataTypeString>()},
        {"event_priv", std::make_shared<DataTypeString>()},
        {"trigger_priv", std::make_shared<DataTypeString>()},
        {"create_tablespace_priv", std::make_shared<DataTypeString>()},
        {"ssl_type", std::make_shared<DataTypeString>()},
        {"ssl_cipher", std::make_shared<DataTypeString>()},
        {"x509_issuer", std::make_shared<DataTypeString>()},
        {"x509_subject", std::make_shared<DataTypeString>()},
        {"max_questions", std::make_shared<DataTypeUInt64>()},
        {"max_updates", std::make_shared<DataTypeUInt64>()},
        {"max_connections", std::make_shared<DataTypeUInt64>()},
        {"max_user_connections", std::make_shared<DataTypeUInt64>()},
        {"plugin", std::make_shared<DataTypeString>()},
        {"authentication_string", std::make_shared<DataTypeString>()},
        {"password_expired", std::make_shared<DataTypeString>()},
    };
    return names_and_types;
}


void StorageSystemCnchUserPriv::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControlManager();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_USERS);

    const auto & manager = context->getAccessControlManager();
    std::vector<UUID> ids = access_control.findAll<User>();

    size_t index = 0;
    auto & column_host = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_user = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_select = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_insert = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_update = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_delete = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_create = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_drop = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_reload = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_shutdown = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_process = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_file = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_grant = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_references = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_index = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_alter = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_show_db = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_super = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_create_tmp_table = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_lock_tables = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_execute = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_repl_slave = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_repl_client = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_create_view = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_show_view = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_create_routine = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_alter_routine = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_create_user = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_event = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_trigger = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_create_tablespace = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_ssl_type = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_ssl_cipher = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_x509_issuer = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_x509_subject = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_max_questions = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & column_max_updates = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & column_max_connections = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & column_max_user_connections = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & column_plugin = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_authentication_string = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_password_expired = assert_cast<ColumnString &>(*res_columns[index++]);

    auto add_row = [&](const String & name,
            const User & user)
    {
        column_host.insertDefault();
        column_user.insertData(name.data(), name.length());

        const String yes = "Y";
        const String no = "N";
        AccessRights access = user.access;
        auto current_roles = user.granted_roles.findGranted(user.default_roles);
        auto current_roles_with_admin_option = user.granted_roles.findGrantedWithAdminOption(user.default_roles);
        auto enabled_roles = manager.getEnabledRoles(current_roles, current_roles_with_admin_option);
        access.makeUnion(enabled_roles->getRolesInfo()->access);

        column_select.insertData(access.isGranted(AccessFlags{AccessType::SELECT}) ? yes.data() : no.data(), 1);
        column_insert.insertData(access.isGranted(AccessFlags{AccessType::INSERT}) ? yes.data() : no.data(), 1);
        column_update.insertData(access.isGranted(AccessFlags{AccessType::ALTER_UPDATE}) ? yes.data() : no.data(), 1);
        column_delete.insertData(access.isGranted(AccessFlags{AccessType::ALTER_DELETE}) ? yes.data() : no.data(), 1);
        column_create.insertData(access.isGranted(AccessFlags{AccessType::CREATE}) ? yes.data() : no.data(), 1);
        column_drop.insertData(access.isGranted(AccessFlags{AccessType::DROP}) ? yes.data() : no.data(), 1);
        /// mysql reload is flush for reloading grant tables
        column_reload.insertData(no.data(), 1);
        column_shutdown.insertData(access.isGranted(AccessFlags{AccessType::SYSTEM_SHUTDOWN}) ? yes.data() : no.data(), 1);
        column_process.insertData(no.data(), 1);
        column_file.insertData(access.isGranted(AccessFlags{AccessType::FILE}) ? yes.data() : no.data(), 1);
        column_grant.insertData(no.data(), 1);
        column_references.insertData(no.data(), 1);
        column_index.insertData(access.isGranted(AccessFlags{AccessType::ALTER_INDEX}) ? yes.data() : no.data(), 1);
        column_alter.insertData(access.isGranted(AccessFlags{AccessType::ALTER}) ? yes.data() : no.data(), 1);
        column_show_db.insertData(access.isGranted(AccessFlags{AccessType::SHOW_DATABASES}) ? yes.data() : no.data(), 1);
        column_super.insertData(access.isGranted(AccessFlags{AccessType::ALL}) ? yes.data() : no.data(), 1);
        column_create_tmp_table.insertData(access.isGranted(AccessFlags{AccessType::CREATE_TEMPORARY_TABLE}) ? yes.data() : no.data(), 1);
        column_lock_tables.insertData(no.data(), 1);
        column_execute.insertData(no.data(), 1);
        column_repl_slave.insertData(no.data(), 1);
        column_repl_client.insertData(no.data(), 1);
        column_create_view.insertData(access.isGranted(AccessFlags{AccessType::CREATE_VIEW}) ? yes.data() : no.data(), 1);
        column_show_view.insertData(access.isGranted(AccessFlags{AccessType::SHOW_TABLES}) ? yes.data() : no.data(), 1);
        column_create_routine.insertData(no.data(), 1);
        column_alter_routine.insertData(no.data(), 1);
        column_create_user.insertData(access.isGranted(AccessFlags{AccessType::CREATE_USER}) ? yes.data() : no.data(), 1);
        column_event.insertData(no.data(), 1);
        column_trigger.insertData(no.data(), 1);
        column_create_tablespace.insertData(no.data(), 1);
        column_ssl_type.insertDefault();
        column_ssl_cipher.insertDefault();
        column_x509_issuer.insertDefault();
        column_x509_subject.insertDefault();
        column_max_questions.insertDefault();
        column_max_updates.insertDefault();
        column_max_connections.insertDefault();
        column_max_user_connections.insertDefault();
        column_plugin.insertDefault();
        column_authentication_string.insertDefault();
        column_password_expired.insertDefault();
    };

    const String & tenant_id = context->getTenantId();
    for (const auto & id : ids)
    {
        auto user = access_control.tryRead<User>(id);
        if (!user)
            continue;

        String user_name = user->getName();
        if (!tenant_id.empty())
        {
            if (!startsWith(user->getName(), tenant_id + "."))
                continue;
            user_name = user_name.substr(tenant_id.length() + 1);
        }

        auto storage = access_control.findStorage(id);
        if (!storage)
            continue;

        add_row(user_name, *user);
    }
}

}
