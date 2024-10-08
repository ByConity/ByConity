#include <sstream>
#include <type_traits>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/VirtualWarehouseHandle.h>
#include <Interpreters/VirtualWarehouseQueue.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTRefreshQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/SQLFingerprint.h>
#include <Storages/StorageMaterializedView.h>
#include "common/logger_useful.h"
#include "Core/SettingsEnums.h"
#include "Core/SettingsFields.h"
#include "Parsers/IAST.h"
#include "Parsers/IAST_fwd.h"

namespace ProfileEvents
{
extern const Event VWQueueMilliseconds;
extern const Event VWQueueType;
}

namespace DB
{
template <class T>
bool matchContainer(T && query_info, T && rule_info, size_t & match_count)
{
    for (auto && ele : rule_info)
    {
        if (std::find(query_info.begin(), query_info.end(), ele) == query_info.end())
            return false;
    }
    if (!rule_info.empty())
        match_count++;
    return true;
}

bool matchString(const std::string & query_info, const std::string & rule_info, size_t & match_count)
{
    if (!rule_info.empty())
    {
        auto ret = query_info == rule_info;
        if (ret)
            match_count++;
        return ret;
    }
    return true;
}
bool matchRegex(const std::string & query_info, const RE2 * re, size_t & match_count)
{
    auto ret = RE2::FullMatch(query_info, *re);
    if (ret)
        match_count++;
    return ret;
}

namespace ErrorCodes
{
    extern const int CNCH_QUEUE_QUERY_FAILURE;
}

void tryGetStorageFromAST(const ASTPtr & ast, ContextMutablePtr & context, StorageInfos & storage_infos)
{
    auto & database_catalog = DatabaseCatalog::instance();
    if (auto * delete_query = ast->as<ASTDeleteQuery>())
    {
        auto database = delete_query->database.empty() ? context->getCurrentDatabase() : delete_query->database;
        storage_infos.databases.push_back(database);
        storage_infos.tables.push_back(database + "." + delete_query->table);
    }
    else if (auto * update = ast->as<ASTUpdateQuery>())
    {
        auto database = update->database.empty() ? context->getCurrentDatabase() : update->database;
        storage_infos.databases.push_back(database);
        storage_infos.tables.push_back(database + "." + update->table);
    }
    else if (auto * insert = ast->as<ASTInsertQuery>())
    {
        //this means the query is function insert, for example `insert into function CnchS3(...)
        if (!insert->table_id.empty())
        {
            auto table_id = insert->table_id;
            if (table_id.database_name.empty())
                table_id.database_name = context->getCurrentDatabase();

            storage_infos.databases.push_back(table_id.database_name);
            storage_infos.tables.push_back(table_id.database_name + "." + table_id.table_name);
        }
    }
    else if (auto * table_expr = ast->as<ASTTableExpression>())
    {
        if (table_expr->database_and_table_name)
        {
            DatabaseAndTableWithAlias db_and_table(table_expr->database_and_table_name);

            auto database = db_and_table.database.empty() ? context->getCurrentDatabase() : db_and_table.database;
            storage_infos.databases.push_back(database);
            storage_infos.tables.push_back(database + "." + db_and_table.table);
        }
    }
    /// XXX: This is a hack solution for an uncommonn query `SELECT ... WHERE x IN table`
    /// which may cause some rare bugs if the identifiers confict with table names.
    /// But I don't want to make the problem complex ..
    else if (auto * func_expr = ast->as<ASTFunction>())
    {
        if (func_expr->arguments && func_expr->arguments->children.size() == 2 && (func_expr->name == "in" || func_expr->name == "notIn"))
        {
            auto * id = func_expr->arguments->children.back()->as<ASTIdentifier>();
            if (id && id->nameParts().size() <= 2)
            {
                String database = id->nameParts().empty() ? "" : id->nameParts().front();
                String table = id->nameParts().empty() ? id->name() : id->nameParts().back();
                storage_infos.databases.push_back(database);
                storage_infos.tables.push_back(database + "." + table);
            }
        }
    }
    else if (auto * refresh_mv = ast->as<ASTRefreshQuery>())
    {
        auto storage = database_catalog.tryGetTable(StorageID(refresh_mv->database, refresh_mv->table), context);
        auto * view_table = dynamic_cast<StorageMaterializedView *>(storage.get());
        if (view_table)
        {
            storage_infos.databases.push_back(refresh_mv->database);
            storage_infos.tables.push_back(refresh_mv->database + "." + refresh_mv->table);
        }
    }

    for (auto & child : ast->children)
    {
        tryGetStorageFromAST(child, context, storage_infos);
    }
}

void enqueueVirtualWarehouseQueue(ContextMutablePtr context, ASTPtr & query_ast)
{
    ASTType ast_type;
    try
    {
        ast_type = query_ast->getType();
    }
    catch (...)
    {
        LOG_DEBUG(getLogger("VirtualWarehouseQueue"), "only queue dml query");
        return;
    }
    if (ast_type != ASTType::ASTSelectQuery && ast_type != ASTType::ASTSelectWithUnionQuery && ast_type != ASTType::ASTInsertQuery
        && ast_type != ASTType::ASTDeleteQuery && ast_type != ASTType::ASTUpdateQuery)
    {
        LOG_DEBUG(getLogger("VirtualWarehouseQueue"), "only queue dml query");
        return;
    }
    auto vw_handle = context->tryGetCurrentVW();
    if (vw_handle && context->getSettingsRef().vw_queue_mode != VWQueueMode::Skip)
    {
        Stopwatch queue_watch;
        queue_watch.start();
        auto query_id = context->getCurrentQueryId();
        auto vw_queue_info = std::make_shared<VWQueueInfo>(context->getCurrentQueryId(), context);
        StorageInfos storage_infos;
        tryGetStorageFromAST(query_ast, context, storage_infos);
        std::copy(storage_infos.databases.begin(), storage_infos.databases.end(), std::back_inserter(vw_queue_info->query_rule.databases));
        std::copy(storage_infos.tables.begin(), storage_infos.tables.end(), std::back_inserter(vw_queue_info->query_rule.tables));
        vw_queue_info->query_rule.query_id = context->getCurrentQueryId();
        vw_queue_info->query_rule.user = context->getClientInfo().current_user;
        vw_queue_info->query_rule.ip = context->getClientInfo().current_address.toString();
        if (query_ast)
        {
            auto fingerprint = SQLFingerprint().generateMD5(query_ast);
            LOG_TRACE(getLogger("VirtualWarehouseQueueManager"), "sql : fingerprint is {}", fingerprint);
            vw_queue_info->query_rule.fingerprint = fingerprint;
        }
        auto queue_result = vw_handle->enqueue(vw_queue_info, context->getSettingsRef().vw_query_queue_timeout_ms);

        ProfileEvents::increment(ProfileEvents::VWQueueMilliseconds, queue_watch.elapsedMilliseconds());
        if (queue_result == VWQueueResultStatus::QueueSuccess)
        {
            LOG_DEBUG(
                getLogger("VirtualWarehouseQueueManager"), "query queue run time : {} ms", queue_watch.elapsedMilliseconds());
        }
        else
        {
            LOG_ERROR(
                getLogger("VirtualWarehouseQueueManager"), "query queue result : {}", VWQueueResultStatusToString(queue_result));
            throw Exception(
                ErrorCodes::CNCH_QUEUE_QUERY_FAILURE,
                "query queue failed for query_id {}: {}",
                query_id,
                VWQueueResultStatusToString(queue_result));
        }
    }
}

String queueRuleToString(const ResourceManagement::QueueRule & queue_rule)
{
    std::stringstream ss;
    ss << "rule_name : " << queue_rule.rule_name << '\t';
    for (const auto & db : queue_rule.databases)
        ss << "db : " << db << '\t';
    for (const auto & table : queue_rule.tables)
        ss << "table : " << table << '\t';
    ss << "ip : " << queue_rule.ip << "\tuser : " << queue_rule.user << "\tquery_id : " << queue_rule.query_id
       << "\tfingerprint : " << queue_rule.fingerprint;
    return ss.str();
}

String queueDataToString(const ResourceManagement::QueueData & queue_data)
{
    std::stringstream ss;
    ss << "queue name : " << queue_data.queue_name << '\t';
    ss << "max_concurrency " << queue_data.max_concurrency << '\t';
    ss << "query_queue_size " << queue_data.query_queue_size << '\t';
    for (const auto & rule : queue_data.queue_rules)
    {
        ss << queueRuleToString(rule);
    }
    return ss.str();
}

const char * VWQueueResultStatusToString(VWQueueResultStatus status)
{
    switch (status)
    {
        case VWQueueResultStatus::QueueSuccess:
            return "QueueSuccess";
        case VWQueueResultStatus::QueueCancel:
            return "QueueCancel";
        case VWQueueResultStatus::QueueFailed:
            return "QueueFailed";
        case VWQueueResultStatus::QueueOverSize:
            return "QueueOverSize";
        case VWQueueResultStatus::QueueStop:
            return "QueueStop";
        case VWQueueResultStatus::QueueTimeOut:
            return "QueueTimeOut";
        default:
            return "QueueUnkown";
    }
}

DequeueRelease::~DequeueRelease()
{
    for (auto & [queue_ptr, count] : enqueue_counts)
        queue_ptr->dequeue(count);
}

void VirtualWarehouseQueue::init(const std::string & queue_name_)
{
    queue_name = queue_name_;
}

VWQueueResultStatus VirtualWarehouseQueue::enqueue(VWQueueInfoPtr queue_info, UInt64 timeout_ms)
{
    std::unique_lock lk(mutex);
    LOG_DEBUG(log, "process query {}, current parallel : {}, max {}", queue_info->query_id, current_parallelize_size, max_concurrency);
    if (max_concurrency == 0 || is_stop)
    {
        queue_info->result = VWQueueResultStatus::QueueFailed;
        return VWQueueResultStatus::QueueFailed;
    }
    if (vw_query_queue.size() == query_queue_size)
    {
        return VWQueueResultStatus::QueueOverSize;
    }
    if (current_parallelize_size < max_concurrency)
    {
        LOG_DEBUG(log, "query {} execute current {}, max {}", queue_info->query_id, current_parallelize_size, max_concurrency);
        current_parallelize_size++;
        auto & dequeue_ptr = queue_info->context->getDequeuePtr();
        if (!dequeue_ptr)
        {
            dequeue_ptr = std::make_shared<DequeueRelease>();
        }
        dequeue_ptr->increase(this);
        return VWQueueResultStatus::QueueSuccess;
    }
    else
    {
        vw_query_queue.push_back(queue_info);
        LOG_DEBUG(log, "query {} enqueue, queue size : {}", queue_info->query_id, vw_query_queue.size());
        if (queue_info->cv.wait_for(lk, std::chrono::milliseconds(timeout_ms)) == std::cv_status::timeout)
        {
            LOG_DEBUG(log, "query {} timeout", queue_info->query_id);
            queue_info->result = VWQueueResultStatus::QueueTimeOut;
            return VWQueueResultStatus::QueueTimeOut;
        }
    }
    LOG_DEBUG(log, "end process query {}, current parallel : {}", queue_info->query_id, current_parallelize_size);
    return queue_info->result;
}

void VirtualWarehouseQueue::dequeue(size_t enqueue_count)
{
    std::unique_lock lk(mutex);
    if (current_parallelize_size >= enqueue_count)
        current_parallelize_size -= enqueue_count;
    else
        current_parallelize_size = 0;
    LOG_DEBUG(log, "dequeue {}, current parallel : {}, max: {}", enqueue_count, current_parallelize_size, max_concurrency);
    if (current_parallelize_size < max_concurrency && !is_stop)
    {
        size_t size = max_concurrency - current_parallelize_size;
        while (size && !vw_query_queue.empty())
        {
            auto iter = vw_query_queue.begin();
            if ((*iter)->result == VWQueueResultStatus::QueueTimeOut)
            {
                LOG_DEBUG(log, "query {} timeout", (*iter)->query_id);
                vw_query_queue.pop_front();
                continue;
            }
            size--;
            current_parallelize_size++;
            LOG_DEBUG(
                log,
                "trigger query {}, current parallel : {} query_size : {}",
                (*iter)->query_id,
                current_parallelize_size,
                vw_query_queue.size());
            auto & dequeue_ptr = (*iter)->context->getDequeuePtr();
            if (!dequeue_ptr)
            {
                dequeue_ptr = std::make_shared<DequeueRelease>();
            }
            dequeue_ptr->increase(this);
            (*iter)->cv.notify_one();
            vw_query_queue.pop_front();
        }
    }
    LOG_DEBUG(log, "end dequeue, current parallel : {}", current_parallelize_size);
}

void VirtualWarehouseQueue::shutdown()
{
    std::unique_lock lk(mutex);
    is_stop = true;
    for (auto iter = vw_query_queue.begin(); iter != vw_query_queue.end();)
    {
        (*iter)->result = VWQueueResultStatus::QueueStop;
        (*iter)->cv.notify_one();
        iter = vw_query_queue.erase(iter);
    }
}

void VirtualWarehouseQueue::updateQueue(const ResourceManagement::QueueData & queue_data)
{
    {
        std::unique_lock<std::shared_mutex> lock(rule_mutex);
        LOG_TRACE(log, "updateQueue {}", queueDataToString(queue_data));
        rules.clear();
        for (const auto & rule : queue_data.queue_rules)
            rules.push_back(rule);
    }

    std::unique_lock lk(mutex);
    max_concurrency = queue_data.max_concurrency;
    query_queue_size = queue_data.query_queue_size;
}

size_t VirtualWarehouseQueue::matchRules(const ResourceManagement::QueueRule & query_rule)
{
    std::shared_lock<std::shared_mutex> lock(rule_mutex);
    bool is_match = false;
    size_t rule_weight_sum = 0;
    for (const auto & rule : rules)
    {
        size_t rule_weight = 0;
        is_match |= matchContainer(query_rule.databases, rule.databases, rule_weight)
            && matchContainer(query_rule.tables, rule.tables, rule_weight)
            && (rule.query_id_regex ? matchRegex(query_rule.query_id, rule.query_id_regex.get(), rule_weight) : true)
            && matchString(query_rule.user, rule.user, rule_weight) && matchString(query_rule.ip, rule.ip, rule_weight)
            && matchString(query_rule.fingerprint, rule.fingerprint, rule_weight);
        LOG_DEBUG(log, "try to match rule {}, is_match {} rule_weight {}", rule.rule_name, is_match, rule_weight);
        rule_weight_sum += rule_weight;
    }
    return is_match ? rule_weight_sum : 0;
}

void VirtualWarehouseQueueManager::init()
{
    for (size_t index = 0; index < static_cast<size_t>(QueueName::Count); index++)
    {
        query_queues[index].init(SettingFieldEnum<QueueName, SettingFieldQueueNameTraits>(static_cast<QueueName>(index)).toString());
    }
}

void VirtualWarehouseQueueManager::updateQueue(const std::vector<ResourceManagement::QueueData> & queue_datas)
{
    for (const auto & queue_data : queue_datas)
    {
        LOG_TRACE(log, "update queue for {}", queue_data.queue_name);
        for (auto & queue : query_queues)
        {
            if (queue.queueName() == queue_data.queue_name)
            {
                queue.updateQueue(queue_data);
            }
        }
    }
}

VWQueueResultStatus VirtualWarehouseQueueManager::enqueue(VWQueueInfoPtr queue_info, UInt64 timeout_ms)
{
    if (is_stop)
        return VWQueueResultStatus::QueueStop;

    LOG_TRACE(log, "vw begin enqueue query_rule {}", queueRuleToString(queue_info->query_rule));

    auto queue_name = queue_info->context->getSettingsRef().queue_name.value;
    if (queue_name < QueueName::Count)
    {
        return query_queues[static_cast<size_t>(queue_name)].enqueue(queue_info, timeout_ms);
    }
    size_t max_rule_weight = 0;
    std::optional<size_t> target_queue_index;
    for (size_t index = 0; index < static_cast<size_t>(QueueName::Count); index++)
    {
        auto rule_weight = query_queues[index].matchRules(queue_info->query_rule);
        if (rule_weight > max_rule_weight)
        {
            max_rule_weight = rule_weight;
            target_queue_index = index;
        }
    }

    if (!target_queue_index)
    {
        if (queue_info->context->getSettingsRef().vw_queue_mode == VWQueueMode::Match)
            return VWQueueResultStatus::QueueSuccess;
        LOG_DEBUG(log, "can't find target queue, use Normal queue");
        target_queue_index = static_cast<size_t>(QueueName::Normal);
    }

    ProfileEvents::increment(ProfileEvents::VWQueueType, *target_queue_index);
    auto result = query_queues[*target_queue_index].enqueue(queue_info, timeout_ms);
    return result;
}

void VirtualWarehouseQueueManager::shutdown()
{
    is_stop = true;
    for (auto & query_queue : query_queues)
    {
        query_queue.shutdown();
    }
}

} // DB
