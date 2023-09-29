#include <metric_collector.h>

#include <algorithm>

#include <metric_client.h>

//DEFINE_int32(enable_debug_metric, 0, "enable debug metric");

namespace metrics2
{

const std::string MetricCollector::kMetricTypeCounter = "counter";
const std::string MetricCollector::kMetricTypeTimer = "timer";
const std::string MetricCollector::kMetricTypeStore = "store";
const std::string MetricCollector::kMetricTypeTsStore = "ts_store";
const std::string MetricCollector::kMetricTypeRateCounter = "rate_counter";
const std::string MetricCollector::kMetricTypeMeter = "meter";

const std::string MetricCollector::kMessageTypeEmit = "emit";
const std::string MetricCollector::kMessageTypeReset = "reset";

int MetricCollector::init(const MetricCollectorConf& conf) {
  namespace_prefix_ = conf.namespace_prefix;
  if (!namespace_prefix_.empty()) {
    namespace_prefix_ += ".";
  }
  has_namespace_prefix_ = true;
  enable_debug_metric_ = conf.enable_debug_metric;
  return MetricClient::init(conf);
}

std::string MetricCollector::make_tagkv(const TagkvList& tagkv_list) {
  std::string ss;
  ss.reserve(1024);
  bool first = true;
  for (auto& kv : tagkv_list) {
    if (first) {
      first = false;
    } else {
      ss.append("|");
    }
    ss.append(kv.first);
    ss.append("=");
    ss.append(kv.second);
  }
  return ss;
}

int MetricCollector::send_emit_message(const std::string& action,
                                       const std::string& type,
                                       const std::string& name, double value,
                                       std::string tagkv, time_t ts) const {
  return MetricClient::send_emit_message(
      action, type, get_canonical_metric_name(name), value, std::move(tagkv),
      use_global_namespace_prefix(name), ts);
}

int MetricCollector::send_reset_message(const std::string& action,
                                        const std::string& type,
                                        const std::string& name, double value,
                                        std::string tagkv) const {
  return MetricClient::send_reset_message(
      action, type, get_canonical_metric_name(name), value, std::move(tagkv),
      use_global_namespace_prefix(name));
}

} /* namespace metrics2 */
