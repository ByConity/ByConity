#pragma once

#include <metrics.h>
#include <metric_collector_conf.h>

namespace Metrics {

enum MetricType
{
    None,
    Counter,
    Meter,
    Rate,
    Store,
    Timer,
    TsStore,
};

typedef std::pair<std::string, std::string> Tag;
typedef std::vector<Tag> Tags;

extern bool isInitialized;

void InitMetrics(const metrics2::MetricCollectorConf& config, const std::string & tags = {});

void EmitCounter(const std::string& name, double value, const std::string& tag = "");

void EmitMeter(const std::string& name, double value, const std::string& tag = "");

void EmitRateCounter(const std::string& name, double value, const std::string& tag = "");

void EmitStore(const std::string& name, double value, const std::string& tag = "");

void EmitTimer(const std::string& name, double value, const std::string& tag = "");

void EmitTsStore(const std::string& name, double value, time_t ts, const std::string& tag = "");

void EmitMetric(const MetricType type, const std::string & name, double value, const std::string & tag, time_t ts);

} // namespace Metrics
