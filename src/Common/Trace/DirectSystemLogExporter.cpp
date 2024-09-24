#include <memory>
#include <unordered_map>
#include <opentelemetry/sdk/trace/span_data.h>
#include <Common/Trace/DirectSystemLogExporter.h>
#include <Common/Trace/TracerUtils.h>
#include <common/logger_useful.h>
#include <DataTypes/DataTypeDate.h>
#include <Columns/ColumnMap.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>

namespace DB
{
NamesAndTypesList OTELTraceLogElement::getNamesAndTypes()
{
  return
    {
        {"event_time", std::make_shared<DataTypeDateTime64>(6)},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"trace_id", std::make_shared<DataTypeString>()},
        {"span_id", std::make_shared<DataTypeString>()},
        {"parent_span_id", std::make_shared<DataTypeString>()},
        {"tracestate", std::make_shared<DataTypeString>()},
        {"span_name", std::make_shared<DataTypeString>()},
        {"span_kind", std::make_shared<DataTypeString>()},
        {"service_name", std::make_shared<DataTypeString>()},
        {"duration_ns", std::make_shared<DataTypeUInt64>()},
        {"status_code", std::make_shared<DataTypeString>()},
        {"status_message", std::make_shared<DataTypeString>()},
        {"resource_attributes", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
        {"span_attributes", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())}
    };
}

void dumpToMapColumn(const std::unordered_map<String, String> & map, DB::IColumn * column)
{
    auto * column_map = column ? &typeid_cast<DB::ColumnMap &>(*column) : nullptr;
    if (!column_map)
        return;

    auto & offsets = column_map->getOffsets();
    auto & key_column = column_map->getKey();
    auto & value_column = column_map->getValue();

    size_t size = 0;
    for (auto & entry : map)
    {
        String value = entry.second;

        key_column.insertData(entry.first.c_str(), strlen(entry.first.c_str()));
        value_column.insert(value);
        size++;
    }

    offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + size);
}

void OTELTraceLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(event_time);
    columns[i++]->insert(DateLUT::serverTimezoneInstance().toDayNum(event_time / 1000000000).toUnderType());
    columns[i++]->insert(trace_id);
    columns[i++]->insert(span_id);
    columns[i++]->insert(parent_span_id);
    columns[i++]->insert(tracestate);
    columns[i++]->insert(span_name);
    columns[i++]->insert(span_kind);
    columns[i++]->insert(service_name);
    columns[i++]->insert(duration_ns);
    columns[i++]->insert(status_code);
    columns[i++]->insert(status_message);

    if (resource_attributes)
    {
        auto * column = columns[i++].get();
        dumpToMapColumn(*resource_attributes, column);
    }
    else
    {
        columns[i++]->insertDefault();
    }

    if (span_attributes)
    {
        auto * column = columns[i++].get();
        dumpToMapColumn(*span_attributes, column);
    }
    else
    {
        columns[i++]->insertDefault();
    }
}

DirectSystemLogExporter::DirectSystemLogExporter(ContextPtr context, const DirectSystemLogExporterOptions & exporter_options) noexcept
{
    const String default_database_name = "system";
    String database = exporter_options.database;
    String table = exporter_options.table;

    if (database != default_database_name)
    {
        /// System tables must be loaded before other tables, but loading order is undefined for all databases except `system`
        LOG_ERROR(
            &Poco::Logger::get("SystemLog"),
            "Custom database name for a system table specified in config."
            " Table `{}` will be created in `system` database instead of `{}`",
            table,
            database);
        database = default_database_name;
    }

    String engine = "ENGINE = MergeTree";
    if (!exporter_options.partition_by_clause.empty())
        engine += " PARTITION BY (" + exporter_options.partition_by_clause + ")";
    if (!exporter_options.ttl_clause.empty())
        engine += " TTL " + exporter_options.ttl_clause;
    engine += " ORDER BY " + exporter_options.order_by_clause;

    // Validate engine definition grammatically to prevent some configuration errors
    ParserStorage storage_parser(ParserSettings::valueOf(context->getSettingsRef()));
    parseQuery(
        storage_parser,
        engine.data(),
        engine.data() + engine.size(),
        "Storage to create table for " + exporter_options.table,
        0,
        DBMS_DEFAULT_MAX_PARSER_DEPTH);

    trace_log = std::make_shared<OTELTraceLog>(context, database, table, engine, exporter_options.flush_interval_milliseconds);

    try
    {
        trace_log->startup();
    }
    catch (...)
    {
        trace_log->shutdown();
    }
}

std::unique_ptr<opentelemetry::sdk::trace::Recordable> DirectSystemLogExporter::MakeRecordable() noexcept
{
    return std::unique_ptr<opentelemetry::sdk::trace::Recordable>(new opentelemetry::sdk::trace::SpanData());
}

 OTELTraceLogElement DirectSystemLogExporter::makeTraceLogElement(std::unique_ptr<opentelemetry::sdk::trace::SpanData> span)
{
    OTELTraceLogElement log_element;
    log_element.event_time = span->GetStartTime().time_since_epoch().count();
    log_element.trace_id = hexEncodeTraceId(span->GetTraceId());
    log_element.span_id = hexEncodeSpanId(span->GetSpanId());
    log_element.span_kind = spanKindToString(span->GetSpanKind());
    log_element.span_name = std::string(span->GetName());
    log_element.duration_ns = span->GetDuration().count();
    auto attributes = span->GetAttributes();
    if (!attributes.empty())
    {
        log_element.span_attributes = std::make_shared<std::unordered_map<String, String>>();
        for (auto & attribute : attributes)
        {
            log_element.span_attributes->emplace(attribute.first, printAttributeValue(attribute.second));
        }
    }

    auto span_resource = span->GetResource();
    if (!span_resource.GetAttributes().empty())
    {
        log_element.resource_attributes = std::make_shared<std::unordered_map<String, String>>();
        for (const auto & attribute : span_resource.GetAttributes())
        {
            log_element.resource_attributes->emplace(attribute.first, printAttributeValue(attribute.second));
        }
    }

    return log_element;
}

opentelemetry::sdk::common::ExportResult DirectSystemLogExporter::Export(
    const opentelemetry::nostd::span<std::unique_ptr<opentelemetry::sdk::trace::Recordable>> & recordables) noexcept
{
    if (isShutdown())
    {
        LOG_WARNING(
            &Poco::Logger::get("DirectSystemLogExporter"),
            fmt::format("[DirectSystemLogExporter] Exporting {} span(s) failed, exporter is shutdown", recordables.size()));
        return opentelemetry::sdk::common::ExportResult::kFailure;
    }

    for (auto & recordable : recordables)
    {
        auto span = std::unique_ptr<opentelemetry::sdk::trace::SpanData>(
            static_cast<opentelemetry::sdk::trace::SpanData *>(recordable.release()));
        if (span != nullptr)
        {
            auto trace_log_element = makeTraceLogElement(std::move(span));
            trace_log->add(trace_log_element);
        }
    }

    return opentelemetry::sdk::common::ExportResult::kSuccess;
}

bool DirectSystemLogExporter::Shutdown(std::chrono::microseconds /*timeout*/) noexcept
{
    const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock);
    is_shutdown = true;
    return true;
}
}
