#pragma once

#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <strings.h>

#include <metric_collector_conf.h>


namespace metrics2
{

class MetricCollector {
 public:
  typedef std::vector<std::pair<std::string, std::string>> TagkvList;

  MetricCollector() = default;
  virtual ~MetricCollector() = default;

  int init(const MetricCollectorConf& conf);

  template <typename T>
  typename std::enable_if<
      !std::is_same<MetricCollectorConf,
                    typename std::remove_cv<
                        typename std::remove_reference<T>::type>::type>::value,
      int>::type
  init(T& conf) {
    MetricCollectorConf tmp_conf;
    tmp_conf.namespace_prefix = conf.get("metrics_namespace_prefix");
    tmp_conf.sock_path = conf.get("sock_path", "/tmp/metric.sock");
    tmp_conf.send_batch_size = atoi(conf.get("send_batch_size", "1").c_str());
    tmp_conf.auto_batch = atoi(conf.get("auto_batch", "1").c_str());
    char* tce_psm = std::getenv("TCE_PSM");
    if (tce_psm) {
      tmp_conf.psm = std::string(tce_psm);
    }
    if (tmp_conf.psm.empty()) {
      tmp_conf.psm = conf.get("psm", "");
    }
    if (tmp_conf.psm.empty()) {
      tmp_conf.psm = conf.get("rpc.psm", "");
    }
    if (tmp_conf.psm.empty()) {
      char* c_psm = std::getenv("LOAD_SERVICE_PSM");
      if (c_psm) {
        tmp_conf.psm = std::string(c_psm);
      } else {
        tmp_conf.psm = "data.default.alert";
      }
    }
    std::string debug_metric = conf.get("enable_debug_metric", "false");
    if (!strcasecmp(debug_metric.c_str(), "true") || !strcasecmp(debug_metric.c_str(), "1")
        || !strcasecmp(debug_metric.c_str(), "on")) {
        enable_debug_metric_ = true;
    }
    return init(tmp_conf);
  }

  /**
   *  tagk: tag name
   */
  int define_tagk(const std::string& tagk) {
    (void)tagk;
    return 0;
  }

  // deprecated
  int define_tagkv(const std::string& tagk,
                   const std::vector<std::string>& tagv_list) {
    (void)tagv_list;
    return define_tagk(tagk);
  }

  int define_counter(const std::string& name) {
    (void)name;
    return 0;
  }

  int define_counter(const std::string& name, const std::string& /*units*/) {
    return define_counter(name);
  }

  int define_rate_counter(const std::string& name) {
    (void)name;
    return 0;
  }

  int define_rate_counter(const std::string& name,
                          const std::string& /*units*/) {
    return define_rate_counter(name);
  }

  int define_meter(const std::string& name) {
    (void)name;
    return 0;
  }

  int define_meter(const std::string& name, const std::string& /*units*/) {
    return define_meter(name);
  }

  int define_timer(const std::string& name) {
    (void)name;
    return 0;
  }

  int define_timer(const std::string& name, const std::string& /*units*/) {
    return define_timer(name);
  }

  int define_store(const std::string& name) {
    (void)name;
    return 0;
  }

  int define_store(const std::string& name, const std::string& /*units*/) {
    return define_store(name);
  }

  int define_ts_store(const std::string& name) {
    (void)name;
    return 0;
  }

  int define_ts_store(const std::string& name, const std::string& /*units*/) {
    return define_ts_store(name);
  }

  int emit_counter(const std::string& name, double value) const {
    return emit_message(kMetricTypeCounter, name, value);
  }

  int emit_counter(const std::string& name, double value,
                   std::string tagkv) const {
    return emit_message(kMetricTypeCounter, name, value, std::move(tagkv));
  }

  int emit_counter(const std::string& name, double value,
                   const TagkvList& tagkv_list) const {
    return emit_message(kMetricTypeCounter, name, value, tagkv_list);
  }

  int emit_rate_counter(const std::string& name, double value) const {
    return emit_message(kMetricTypeRateCounter, name, value);
  }

  int emit_rate_counter(const std::string& name, double value,
                        const std::string& tagkv) const {
    return emit_message(kMetricTypeRateCounter, name, value, tagkv);
  }

  int emit_rate_counter(const std::string& name, double value,
                        const TagkvList& tagkv_list) {
    return emit_message(kMetricTypeRateCounter, name, value, tagkv_list);
  }

  int emit_meter(const std::string& name, double value) const {
    return emit_message(kMetricTypeMeter, name, value);
  }

  int emit_meter(const std::string& name, double value,
                 const std::string& tagkv) const {
    return emit_message(kMetricTypeMeter, name, value, tagkv);
  }

  int emit_meter(const std::string& name, double value,
                 const TagkvList& tagkv_list) {
    return emit_message(kMetricTypeMeter, name, value, tagkv_list);
  }

  int emit_timer(const std::string& name, double value) const {
    return emit_message(kMetricTypeTimer, name, value);
  }

  int emit_timer(const std::string& name, double value,
                 std::string tagkv) const {
    return emit_message(kMetricTypeTimer, name, value, std::move(tagkv));
  }

  int emit_timer(const std::string& name, double value,
                 const TagkvList& tagkv_list) const {
    return emit_message(kMetricTypeTimer, name, value, tagkv_list);
  }

  int emit_store(const std::string& name, double value) const {
    return emit_message(kMetricTypeStore, name, value);
  }

  int emit_store(const std::string& name, double value,
                 std::string tagkv) const {
    return emit_message(kMetricTypeStore, name, value, std::move(tagkv));
  }

  int emit_store(const std::string& name, double value,
                 const TagkvList& tagkv_list) const {
    return emit_message(kMetricTypeStore, name, value, tagkv_list);
  }

  int emit_ts_store(const std::string& name, double value, time_t ts) const {
    return emit_message(kMetricTypeTsStore, name, value, ts);
  }

  int emit_ts_store(const std::string& name, double value, time_t ts,
                    std::string tagkv) const {
    return emit_message(kMetricTypeTsStore, name, value, std::move(tagkv), ts);
  }

  int emit_ts_store(const std::string& name, double value, time_t ts,
                    const TagkvList& tagkv_list) const {
    return emit_message(kMetricTypeTsStore, name, value, tagkv_list, ts);
  }

  int reset_counter(const std::string& name) const {
    return reset_message(kMetricTypeCounter, name);
  }

  int reset_counter(const std::string& name, std::string tagkv) const {
    return reset_message(kMetricTypeCounter, name, std::move(tagkv));
  }

  int reset_counter(const std::string& name,
                    const TagkvList& tagkv_list) const {
    return reset_message(kMetricTypeCounter, name, tagkv_list);
  }

  int reset_rate_counter(const std::string& name) const {
    return reset_message(kMetricTypeRateCounter, name);
  }

  int reset_rate_counter(const std::string& name, const std::string& tagkv) {
    return reset_message(kMetricTypeRateCounter, name, tagkv);
  }

  int reset_rate_counter(const std::string& name, const TagkvList& tagkv_list) {
    return reset_message(kMetricTypeRateCounter, name, tagkv_list);
  }

  int reset_timer(const std::string& name) const {
    return reset_message(kMetricTypeTimer, name);
  }

  int reset_timer(const std::string& name, std::string tagkv) const {
    return reset_message(kMetricTypeTimer, name, std::move(tagkv));
  }

  int reset_timer(const std::string& name, const TagkvList& tagkv_list) const {
    return reset_message(kMetricTypeTimer, name, tagkv_list);
  }

  int reset_store(const std::string& name) const {
    return reset_message(kMetricTypeStore, name);
  }

  int reset_store(const std::string& name, std::string tagkv) const {
    return reset_message(kMetricTypeStore, name, std::move(tagkv));
  }

  int reset_store(const std::string& name, const TagkvList& tagkv_list) const {
    return reset_message(kMetricTypeStore, name, tagkv_list);
  }

  int reset_ts_store(const std::string& name) const {
    return reset_message(kMetricTypeTsStore, name);
  }

  int reset_ts_store(const std::string& name, std::string tagkv) const {
    return reset_message(kMetricTypeTsStore, name, std::move(tagkv));
  }

  int reset_ts_store(const std::string& name,
                     const TagkvList& tagkv_list) const {
    return reset_message(kMetricTypeTsStore, name, tagkv_list);
  }

  // deprecated
  static int start_flush_thread() { return 1; }

  // deprecated
  static int start_listening_thread() { return 1; }

  static std::string make_tagkv(const TagkvList& tagkv_list);
 protected:

  void emit_debug_metric(const std::string& name) const {
    if (enable_debug_metric_) {
      send_emit_message(kMessageTypeEmit, kMetricTypeCounter,
                        "metric_emit_count", 1, make_tagkv({{"name", name}}),
                        -1);
    }
  }

  int emit_message(const std::string& type, const std::string& name,
                   double value, time_t ts = -1) const {
    emit_debug_metric(name);
    return send_emit_message(kMessageTypeEmit, type, name, value, "", ts);
  }

  int emit_message(const std::string& type, const std::string& name,
                   double value, std::string tagkv, time_t ts = -1) const {
    emit_debug_metric(name);
    return send_emit_message(kMessageTypeEmit, type, name, value,
                             std::move(tagkv), ts);
  }

  int emit_message(const std::string& type, const std::string& name,
                   double value, const TagkvList& tagkv_list,
                   time_t ts = -1) const {
    emit_debug_metric(name);
    return send_emit_message(kMessageTypeEmit, type, name, value,
                             make_tagkv(tagkv_list), ts);
  }

  int reset_message(const std::string& type, const std::string& name) const {
    return send_reset_message(kMessageTypeReset, type, name, 0.0, "");
  }

  int reset_message(const std::string& type, const std::string& name,
                    std::string tagkv) const {
    return send_reset_message(kMessageTypeReset, type, name, 0.0,
                              std::move(tagkv));
  }

  int reset_message(const std::string& type, const std::string& name,
                    const TagkvList& tagkv_list) const {
    return send_reset_message(kMessageTypeReset, type, name, 0.0,
                              make_tagkv(tagkv_list));
  }

  // TODO:
  virtual int send_emit_message(const std::string& action, const std::string& type,
                        const std::string& name, double value,
                        std::string tagkv, time_t ts = -1) const;

  // TODO:
  virtual int send_reset_message(const std::string& action, const std::string& type,
                         const std::string& name, double value,
                         std::string tagkv) const;

  std::string get_canonical_metric_name(const std::string& name) const {
    if (!name.empty() && name[0] == ':') {
      return name.substr(1);
    }
    if (has_namespace_prefix_) {
      return namespace_prefix_ + name;
    }
    return name;
  }

  bool use_global_namespace_prefix(const std::string& name) const {
    return !(!name.empty() && name[0] == ':') && !has_namespace_prefix_;
  }

 public:
  static const std::string kMetricTypeCounter;
  static const std::string kMetricTypeTimer;
  static const std::string kMetricTypeStore;
  static const std::string kMetricTypeTsStore;
  static const std::string kMetricTypeRateCounter;
  static const std::string kMetricTypeMeter;

  static const std::string kMessageTypeEmit;
  static const std::string kMessageTypeReset;

 protected:
  std::string namespace_prefix_;
  bool has_namespace_prefix_ = false;
  std::string _psm;
  bool enable_debug_metric_ = false;
};

} /* namespace metrics2 */
