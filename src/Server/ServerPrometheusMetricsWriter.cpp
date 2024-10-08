#include <Catalog/Catalog.h>
#include <Interpreters/Context.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchServerClient.h>
#include <ResourceGroup/IResourceGroupManager.h>
#include <Server/PrometheusMetricsWriter.h>
#include <Server/ServerPrometheusMetricsWriter.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Common/HistogramMetrics.h>
#include <Common/LabelledMetrics.h>
#include <Common/RpcClientPool.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/config_version.h>
#include <Interpreters/Context_fwd.h>

#include <algorithm>
#include <atomic>

namespace DB
{

using DataModelTablePartitionInfos = std::vector<DB::Protos::DataModelTablePartitionInfo>;

ServerPrometheusMetricsWriter::ServerPrometheusMetricsWriter(
    const Poco::Util::AbstractConfiguration & config, ContextMutablePtr context_,
    const std::string & config_name, const AsynchronousMetrics & async_metrics_)
    : context(context_)
    , async_metrics(async_metrics_)
    , max_concurrent_default_queries(config.getInt("max_concurrent_queries", 0))
    , max_concurrent_insert_queries(config.getInt("max_concurrent_insert_queries", 0))
    , max_concurrent_system_queries(config.getInt("max_concurrent_system_queries", 0))
    , send_events(config.getBool(config_name + ".events", true))
    , send_metrics(config.getBool(config_name + ".metrics", true))
    , send_asynchronous_metrics(config.getBool(config_name + ".asynchronous_metrics", true))
    , send_part_metrics(config.getBool(config_name + ".part_metrics", false))
{}

/**
 * Retrieves InternalMetrics
 */
ServerPrometheusMetricsWriter::MetricMap ServerPrometheusMetricsWriter::getInternalMetrics()
{
    ServerPrometheusMetricsWriter::MetricMap internal_metrics_nameval_map;


    if (context->tryGetResourceGroupManager())
    {
        ResourceGroupInfoVec infos = context->tryGetResourceGroupManager()->getInfoVec();
        for (const auto & info : infos)
        {
            /// Metrics from parent resource groups
            if (info.parent_resource_group.empty())
            {
                MetricLabels labels {{"vw_id", info.name}};
                auto queued_time_label_val_pair = MetricEntry{labels, COUNTER_TYPE, info.queued_time_total_ms};
                auto running_time_label_val_pair = MetricEntry{labels, COUNTER_TYPE, info.running_time_total_ms};
                internal_metrics_nameval_map[QUEUED_QUERIES_TIME_TOTAL_KEY].emplace_back(queued_time_label_val_pair);
                internal_metrics_nameval_map[RUNNING_QUERIES_TIME_TOTAL_KEY].emplace_back(running_time_label_val_pair);

                internal_metrics_nameval_map[QUEUED_QUERIES_KEY].emplace_back(MetricEntry{labels, GAUGE_TYPE, info.queued_queries});
                internal_metrics_nameval_map[RUNNING_QUERIES_KEY].emplace_back(MetricEntry{labels, GAUGE_TYPE, info.running_queries});
            }
        }
    }

    MetricLabels empty_label;

    /// Queries which are going to be executed on the worker side

    // TODO:lianwenlong
    // internal_metrics_nameval_map[INTENT_QUERIES_ONDEMAND_KEY].emplace_back(MetricEntry{empty_label, GAUGE_TYPE, context.getOndemandIntentQueries().size()});
    // internal_metrics_nameval_map[INTENT_QUERIES_PREALLOCATE_KEY].emplace_back(MetricEntry{empty_label, GAUGE_TYPE, context.getNumPreallocateIntentQueries()});

    // /// `CurrentMetrics::Manipulation` indicates the number of data manipulation including the number of background merge tasks
    /// Only when there is no executing queries, no background tasks and no intent queries, then the node can be shut down.
    bool ready_to_shut_down_val = !(CurrentMetrics::values[CurrentMetrics::DefaultQuery].load(std::memory_order_relaxed));
                                    // || internal_metrics_nameval_map[INTENT_QUERIES_ONDEMAND_KEY].back().value
                                    // || internal_metrics_nameval_map[INTENT_QUERIES_PREALLOCATE_KEY].back().value);

    internal_metrics_nameval_map[READY_TO_SHUT_DOWN_KEY].emplace_back(MetricEntry{empty_label, GAUGE_TYPE, ready_to_shut_down_val});

    MetricLabels uptime_labels;
    if (getenv(POD_UUID_ENVVAR) && getenv(VW_ID_ENVVAR))
    {
        uptime_labels =
        {
            {"pod_uuid", getenv(POD_UUID_ENVVAR)},
            {"vw_id", getenv(VW_ID_ENVVAR)}
        };
    }
    internal_metrics_nameval_map[UPTIME_SECONDS_KEY].emplace_back(MetricEntry{uptime_labels, COUNTER_TYPE, static_cast<UInt64>(context->getUptimeSeconds())});

    return internal_metrics_nameval_map;
}

String getPartMetricKeyLabel(const String & database, const String & table, const String partition_id,
                                const String partition, const String first_partition = "", const String & type = "")
{
    String account_id = "";
    String db_name = database;

    size_t found = database.find_first_of(DB_NAME_DELIMITER);
    if (found != std::string::npos)
    {
        account_id = database.substr(0, found);
        db_name = database.substr(found + 1);
    }

    MetricLabels labels
    {
        {"account_id", account_id},
        {"database", db_name},
        {"table", table},
        {"partition_id", partition_id},
        {"partition", partition},
    };

    if (!type.empty())
    {
        labels.insert({"type", type});
    }
    else
    {
        labels.insert({"first_partition", first_partition});
    }

    return getLabel(labels);

}

void ServerPrometheusMetricsWriter::writeConfigMetrics(WriteBuffer & wb)
{

    for (const auto & [metric_name, metric_doc] : config_namedoc_map)
    {
        std::string key{CONFIG_METRIC_PREFIX + metric_name};
        std::string key_label(key);
        writeOutLine(wb, "# HELP", key, metric_doc);
        writeOutLine(wb, "# TYPE", key, COUNTER_TYPE);

        if (metric_name == BUILD_INFO_KEY)
        {
            MetricLabels labels {
                {"revision", std::to_string(revision)},
                {"version_scm", VERSION_SCM},
                {"version_githash", VERSION_GITHASH}};

            key_label += getLabel(labels);
            writeOutLine(wb, key_label, 1); //Output arbitary value as only the labels are useful
        }
        else if (metric_name == MAX_CONCURRENT_DEFAULT_QUERIES_KEY)
            writeOutLine(wb, key_label, max_concurrent_default_queries);
        else if(metric_name == MAX_CONCURRENT_INSERT_QUERIES_KEY)
        {
            writeOutLine(wb, key_label, max_concurrent_insert_queries);
        }
        else if(metric_name == MAX_CONCURRENT_SYSTEM_QUERIES_KEY)
        {
            writeOutLine(wb, key_label, max_concurrent_system_queries);
        }
        else
        {
            LOG_WARNING(getLogger("ServerPrometheusMetricsWriter"), "Unknown config metric found, this should never happen");
            writeOutLine(wb, key_label, 0);
        }
    }
}


void ServerPrometheusMetricsWriter::writeProfileEvents(WriteBuffer & wb)
{
    auto counters_snapshot = ProfileEvents::global_counters.getPartiallyAtomicSnapshot();

    for (const auto & profile_event : catalog_profile_events_list)
    {
        const auto counter = counters_snapshot[profile_event];

        std::string metric_name{ProfileEvents::getSnakeName(profile_event)};
        metric_name.append(TOTAL_SUFFIX);
        std::string metric_doc{ProfileEvents::getDocumentation(profile_event)};

        replaceInvalidChars(metric_name);
        std::string key{CATALOG_METRICS_PREFIX + metric_name};
        std::string label(key);

        writeOutLine(wb, "# HELP", key, metric_doc);
        writeOutLine(wb, "# TYPE", key, COUNTER_TYPE);
        writeOutLine(wb, label, counter);
    }

    for (const auto & profile_event : profile_events_list)
    {
        auto write_profile_event = [this, &wb, &profile_event](ProfileEvents::Count counter)
        {
            std::string metric_name{ProfileEvents::getSnakeName(profile_event)};
            metric_name.append(TOTAL_SUFFIX);
            std::string metric_doc{ProfileEvents::getDocumentation(profile_event)};

            replaceInvalidChars(metric_name);
            std::string key{PROFILE_EVENTS_PREFIX + metric_name};
            MetricLabels labels;

            if (startsWith(metric_name, sd_request_metric))
            {
                const String pod = DNSResolver::instance().getHostName();
                const String mode = context->getServiceDiscoveryClient()->getName();
                const String cluster = context->getServiceDiscoveryClient()->getClusterName();
                labels.insert(
                    {{"pod", pod},
                    {"mode", mode},
                    {"cluster", cluster}});
            }
            else if (profile_event == ProfileEvents::TSOError)
            {
                labels.insert({"tso_leader_endpoint", context->tryGetTSOLeaderHostPort()});
            }
            std::string key_label = key + getLabel(labels);

            writeOutLine(wb, "# HELP", key, metric_doc);
            writeOutLine(wb, "# TYPE", key, COUNTER_TYPE);
            writeOutLine(wb, key_label, counter);
        };

        const auto counter = counters_snapshot[profile_event];
        write_profile_event(counter);
    }
}

void ServerPrometheusMetricsWriter::writeLabelledMetrics(WriteBuffer & wb)
{
    for (LabelledMetrics::Metric metric = 0; metric < LabelledMetrics::end(); ++metric)
    {
        String metric_name { LabelledMetrics::getSnakeName(metric) };
        String metric_doc { LabelledMetrics::getDocumentation(metric) };

        auto write_metric = [&](MetricLabels labels, LabelledMetrics::Count counter) {
            String key;
            if (metric == LabelledMetrics::VwQuery || metric == LabelledMetrics::UnlimitedQuery)
            {
                labels.insert({"resource_type", metric == LabelledMetrics::VwQuery ? "vw" : "unlimited"});
                key = String(PROFILE_EVENTS_PREFIX) + PROFILE_EVENTS_LABELLED_QUERY_KEY + TOTAL_SUFFIX;
            }
            else if (startsWith(metric_name, failed_queries_metrics))
            {
                /// Combine all different QueriesFailed related events to the same one, and use `type` label to categorize them.
                labels.insert({"failure_type", String(LabelledMetrics::getName(metric))});
                key = String(PROFILE_EVENTS_PREFIX) + failed_queries_metrics + TOTAL_SUFFIX;
                metric_doc = String(LabelledMetrics::getDocumentation(LabelledMetrics::QueriesFailed));
            }
            else
            {
               return;
            }

            String key_label = key + getLabel(labels);
            writeOutLine(wb, "# HELP", key, metric_doc);
            writeOutLine(wb, "# TYPE", key, COUNTER_TYPE);
            writeOutLine(wb, key_label, counter);
        };

        LabelledMetrics::LabelledCounter labelled_counter = LabelledMetrics::getCounter(metric);
        if (!labelled_counter.empty())
        {
            for (const auto & item : labelled_counter)
            {
                
                MetricLabels labels = item.first;
                LabelledMetrics::Count counter = item.second;

                write_metric(labels, counter);
            }
        }
        else
        {
            write_metric({}, 0);
        }
    }
}

void ServerPrometheusMetricsWriter::writeCurrentMetrics(WriteBuffer & wb)
{
    for (const auto & current_metric : current_metrics_list)
    {
        const auto value = CurrentMetrics::values[current_metric].load(std::memory_order_relaxed);

        std::string metric_name{CurrentMetrics::getSnakeName(current_metric)};
        std::string metric_doc{CurrentMetrics::getDocumentation(current_metric)};

        std::string key{CURRENT_METRICS_PREFIX + metric_name};
        std::string label(key);

        if (current_metric == CurrentMetrics::DefaultQuery
            || current_metric == CurrentMetrics::InsertQuery
            || current_metric == CurrentMetrics::SystemQuery)
        {
            key = String{CURRENT_METRICS_PREFIX} + CURRENT_METRICS_QUERY_KEY;
            label = key;

            if (current_metric == CurrentMetrics::DefaultQuery)
                label += getLabel({{"query_type", "default"}});
            if (current_metric == CurrentMetrics::InsertQuery)
                label += getLabel({{"query_type", "insert"}});
            if (current_metric == CurrentMetrics::SystemQuery)
                label += getLabel({{"query_type", "system"}});
        }

        writeOutLine(wb, "# HELP", key, metric_doc);
        writeOutLine(wb, "# TYPE", key, GAUGE_TYPE);
        writeOutLine(wb, label, value);
    }
}

void ServerPrometheusMetricsWriter::writeAsyncMetrics(WriteBuffer & wb)
{
    auto async_metrics_values = async_metrics.getValues();
    for (const auto & async_metric : async_metrics_list)
    {
        std::string key{ASYNCHRONOUS_METRICS_PREFIX + async_metric};

        replaceInvalidChars(key);
        convertCamelToSnake(key);
        auto value = async_metrics_values[async_metric];

        // TODO: add HELP section? asynchronous_metrics contains only key and value
        writeOutLine(wb, "# TYPE", key, GAUGE_TYPE);
        writeOutLine(wb, key, value);
    }
}

void ServerPrometheusMetricsWriter::writeHistogramMetrics(WriteBuffer & wb)
{
    // For histogram metrics
    for (const auto & histogram_metric : histogram_metrics_list)
    {
        // Do not output certain metrics on workers
        if ((histogram_metric == HistogramMetrics::QueryLatency
            || histogram_metric == HistogramMetrics::UnlimitedQueryLatency)
            && context->getServerType() == ServerType::cnch_worker)
            continue;

        HistogramMetrics::LabelledHistogram histogram_labelled_metrics;

        {
            auto lock = HistogramMetrics::getLock(histogram_metric);
            histogram_labelled_metrics = HistogramMetrics::histogram_metrics[histogram_metric];
        }

        // If no metrics have been inserted
        if (histogram_labelled_metrics.empty())
            continue;

        std::string metric_name{HistogramMetrics::getSnakeName(histogram_metric)};
        std::string metric_doc{HistogramMetrics::getDocumentation(histogram_metric)};
        const auto histogram_bucket = HistogramMetrics::getHistogramBucketLimits(histogram_metric);

        std::string key{HISTOGRAM_METRICS_PREFIX + metric_name};
        std::string key_label(key + BUCKET_SUFFIX);
        std::string sum_key(key + SUM_SUFFIX);
        std::string count_key(key + COUNT_SUFFIX);

        writeOutLine(wb, "# HELP", key, metric_doc);
        writeOutLine(wb, "# TYPE", key, HISTOGRAM_TYPE);
        for (const auto & it : histogram_labelled_metrics)
        {
            UInt64 count = 0;

            const auto labels = it.first;
            const auto it_values = it.second.values;

            for (size_t i = 0; i < it_values.size(); ++i)
            {
                const auto & value = it_values[i];
                count += value;
                auto bucket = histogram_bucket[i];
                String metric_key_label(key_label);
                MetricLabels bucket_labels = labels;
                if (i != it_values.size() - 1)
                    bucket_labels.insert({"le", std::to_string(bucket)});
                else
                    bucket_labels.insert({"le", "Inf"});
                metric_key_label += getLabel(bucket_labels);
                writeOutLine(wb, metric_key_label, count);
            }
            String count_key_label{count_key};
            if (!labels.empty())
                count_key_label += getLabel(labels);
            writeOutLine(wb, count_key_label, count);
            String sum_key_label {sum_key};
            if (!labels.empty())
                sum_key_label += getLabel(labels);
            writeOutLine(wb, sum_key_label, it.second.sum);
        }
    }
}

void ServerPrometheusMetricsWriter::writeInternalMetrics(WriteBuffer & wb)
{
    auto internal_metrics_map = getInternalMetrics();
    for (const auto & internal_metric_keyval : internal_metrics_namedoc_map)
    {
        auto internal_metric = internal_metric_keyval.first;
        auto metric_doc = internal_metric_keyval.second;

        for (const auto & metric_entry : internal_metrics_map[internal_metric])
        {
            std::string key{INTERNAL_METRICS_PREFIX + internal_metric};
            std::string label(key);
            if (!metric_entry.labels.empty())
                label += getLabel(metric_entry.labels);

            writeOutLine(wb, "# HELP", key, metric_doc);
            writeOutLine(wb, "# TYPE", key, metric_entry.type);
            writeOutLine(wb, label, metric_entry.value);
        }
    }
}

void ServerPrometheusMetricsWriter::writePartMetrics(WriteBuffer & wb)
{
    if (context->getServerType() == ServerType::cnch_server && send_part_metrics)
    {
        auto curr_ts = context->getTimestamp();
        auto cnch_catalog = context->getCnchCatalog();

        if (!cnch_catalog)
        {
            LOG_WARNING(getLogger("ServerPrometheusMetricsWriter"), "Cannot get catalog for part metrics");
        }
        else
        {
            auto db_models = cnch_catalog->getAllDataBases();

            std::string parts_num_key(PART_METRICS_PREFIX);
            parts_num_key.append(PARTS_NUMBER_LABEL);
            std::string parts_size_key(PART_METRICS_PREFIX);
            parts_size_key.append(PARTS_SIZE_LABEL);
            std::string rows_count_key(PART_METRICS_PREFIX);
            rows_count_key.append(ROWS_COUNT_LABEL);

            for (auto & db_model : db_models)
            {
                auto & db_name = db_model.name();

                auto tables = cnch_catalog->getTablesInDB(db_name);
                for (auto & table_name : tables)
                {
                    DB::StoragePtr storage = cnch_catalog->tryGetTable(*context, db_name, table_name, TxnTimestamp{context->getTimestamp()});
                    auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(storage.get());
                    if (!cnch_table)
                        continue;

                    Catalog::PartitionMap partitions;
                    cnch_catalog->getPartitionsFromMetastore(*cnch_table, partitions, nullptr);

                    for (auto & partition : partitions)
                    {
                        WriteBufferFromOwnString out;
                        partition.second->partition_ptr->serializeText(*cnch_table, out, format_settings);
                        auto partition_name = out.str();
                        auto partition_id = partition.first;

                        auto all_parts = cnch_catalog->getServerDataPartsInPartitions(storage, {partition_id}, curr_ts, nullptr);
                        auto visible_parts = CnchPartsHelper::calcVisibleParts(all_parts, false);
                        auto current_visible = visible_parts.cbegin();

                        /// Total parts
                        UInt64 total_parts_num = all_parts.size();
                        UInt64 total_parts_size = 0;
                        UInt64 total_rows_count = 0;
                        /// Visible parts
                        UInt64 visible_parts_num = visible_parts.size();
                        UInt64 visible_parts_size = 0;
                        UInt64 visible_rows_count = 0;
                        /// Invisible parts
                        UInt64 invisible_parts_num = total_parts_num - visible_parts_num;
                        UInt64 invisible_parts_size = 0;
                        UInt64 invisible_rows_count = 0;

                        //get CNCH parts
                        for (size_t i = 0; i != total_parts_num; ++i)
                        {
                            auto part_size = all_parts[i]->part_model().size();
                            auto rows_count = all_parts[i]->part_model().rows_count();

                            total_parts_size += part_size;
                            total_rows_count += rows_count;

                            /// all_parts and visible_parts are both ordered by info.
                            /// For each part within all_parts, we find the first part greater than or equal to it in visible_parts,
                            /// which means the part is visible if they are equal to each other
                            while (current_visible != visible_parts.cend() && (*current_visible)->info() < all_parts[i]->info()) ++current_visible;
                            if (current_visible != visible_parts.cend() && all_parts[i]->info() == (*current_visible)->info())
                            {
                                visible_parts_size += part_size;
                                visible_rows_count += rows_count;
                            }
                            else
                            {
                                invisible_parts_size += part_size;
                                invisible_rows_count += rows_count;
                            }
                        }

                        auto total_label = getPartMetricKeyLabel(db_name, table_name, partition_id, partition_name, "", "total");
                        /// Total parts num metrics
                        writeOutLine(wb, "# HELP", parts_num_key, part_metrics_namedoc_map.at(PARTS_NUMBER_LABEL));
                        writeOutLine(wb, "# TYPE", parts_num_key, GAUGE_TYPE);
                        std::string total_parts_num_key_label{parts_num_key};
                        total_parts_num_key_label += total_label;
                        writeOutLine(wb, total_parts_num_key_label, total_parts_num);
                        /// Total parts size metrics
                        writeOutLine(wb, "# HELP", parts_size_key, part_metrics_namedoc_map.at(PARTS_SIZE_LABEL));
                        writeOutLine(wb, "# TYPE", parts_size_key, GAUGE_TYPE);

                        std::string total_parts_size_key_label{parts_size_key};
                        total_parts_size_key_label += total_label;
                        writeOutLine(wb, total_parts_size_key_label, total_parts_size);
                        /// Total rows count metrics
                        writeOutLine(wb, "# HELP", rows_count_key, part_metrics_namedoc_map.at(ROWS_COUNT_LABEL));
                        writeOutLine(wb, "# TYPE", rows_count_key, GAUGE_TYPE);
                        std::string total_rows_count_key_label{rows_count_key};
                        total_rows_count_key_label += total_label;
                        writeOutLine(wb, total_rows_count_key_label, total_rows_count);

                        /// Visible parts num metrics
                        auto visible_label = getPartMetricKeyLabel(db_name, table_name, partition_id, partition_name, "", "visible");
                        writeOutLine(wb, "# HELP", parts_num_key, part_metrics_namedoc_map.at(PARTS_NUMBER_LABEL));
                        writeOutLine(wb, "# TYPE", parts_num_key, GAUGE_TYPE);
                        std::string visible_parts_num_key_label{parts_num_key};
                        visible_parts_num_key_label += visible_label;
                        writeOutLine(wb, visible_parts_num_key_label, visible_parts_num);
                        /// Visible parts size metrics
                        writeOutLine(wb, "# HELP", parts_size_key, part_metrics_namedoc_map.at(PARTS_SIZE_LABEL));
                        writeOutLine(wb, "# TYPE", parts_size_key, GAUGE_TYPE);
                        std::string visible_parts_size_key_label{parts_size_key};
                        visible_parts_size_key_label += visible_label;
                        writeOutLine(wb, visible_parts_size_key_label, visible_parts_size);
                        /// Visible rows count metrics
                        writeOutLine(wb, "# HELP", rows_count_key, part_metrics_namedoc_map.at(ROWS_COUNT_LABEL));
                        writeOutLine(wb, "# TYPE", rows_count_key, GAUGE_TYPE);
                        std::string visible_rows_count_key_label{rows_count_key};
                        visible_rows_count_key_label += visible_label;
                        writeOutLine(wb, visible_rows_count_key_label, visible_rows_count);

                        /// Invisible parts num metrics
                        auto invisible_label = getPartMetricKeyLabel(db_name, table_name, partition_id, partition_name, "", "invisible");
                        writeOutLine(wb, "# HELP", parts_num_key, part_metrics_namedoc_map.at(PARTS_NUMBER_LABEL));
                        writeOutLine(wb, "# TYPE", parts_num_key, GAUGE_TYPE);
                        std::string invisible_parts_num_key_label{parts_num_key};
                        invisible_parts_num_key_label += invisible_label;
                        writeOutLine(wb, invisible_parts_num_key_label, invisible_parts_num);
                        /// Invisible parts size metrics
                        writeOutLine(wb, "# HELP", parts_size_key, part_metrics_namedoc_map.at(PARTS_SIZE_LABEL));
                        writeOutLine(wb, "# TYPE", parts_size_key, GAUGE_TYPE);
                        std::string invisible_parts_size_key_label{parts_size_key};
                        invisible_parts_size_key_label += invisible_label;
                        writeOutLine(wb, invisible_parts_size_key_label, invisible_parts_size);
                        /// Invisible rows count metrics
                        writeOutLine(wb, "# HELP", rows_count_key, part_metrics_namedoc_map.at(ROWS_COUNT_LABEL));
                        writeOutLine(wb, "# TYPE", rows_count_key, GAUGE_TYPE);
                        std::string invisible_rows_count_key_label{rows_count_key};
                        invisible_rows_count_key_label += invisible_label;
                        writeOutLine(wb, invisible_rows_count_key_label, invisible_rows_count);
                    }
                }
            }
        }
    }

}

void ServerPrometheusMetricsWriter::write(WriteBuffer & wb)
{
    writeConfigMetrics(wb);

    writeLabelledMetrics(wb);

    if (send_events)
        writeProfileEvents(wb);

    if (send_metrics)
        writeCurrentMetrics(wb);

    if (send_asynchronous_metrics)
        writeAsyncMetrics(wb);

    writeHistogramMetrics(wb);
    writeInternalMetrics(wb);

    /// Export the parts related metrics, the values are consistent with the system.cnch_parts
    writePartMetrics(wb);
}
}
