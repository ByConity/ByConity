#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <metric_collector.h>
#include <metric_collector_conf.h>

namespace metrics2
{

class Metrics {
 public:
  typedef std::vector<std::pair<std::string, std::string>> TagkvList;

  Metrics() = delete;
  ~Metrics() = delete;

  static int init(const MetricCollectorConf& conf);

  template <typename T>
  static /* int */
      typename std::enable_if<
          !std::is_same<MetricCollectorConf,
                        typename std::remove_cv<typename std::remove_reference<
                            T>::type>::type>::value,
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
    return init(tmp_conf);
  }

  static int init(const std::string& prefix, const std::string& psm,
                  int batch_size = 128, int auto_batch = 1) {
    MetricCollectorConf tmp_conf;
    tmp_conf.namespace_prefix = prefix;
    tmp_conf.sock_path = "/tmp/metric.sock";
    tmp_conf.send_batch_size = batch_size;
    tmp_conf.auto_batch = auto_batch;
    tmp_conf.psm = psm;
    if (tmp_conf.psm.empty()) {
      char* tce_psm = std::getenv("TCE_PSM");
      if (tce_psm != nullptr) {
        tmp_conf.psm = std::string(tce_psm);
      } else {
        char* c_psm = std::getenv("LOAD_SERVICE_PSM");
        if (c_psm != nullptr) {
          tmp_conf.psm = std::string(c_psm);
        } else {
          tmp_conf.psm = "data.default.alert";
        }
      }
    }
    return init(tmp_conf);
  }

  /**
   *  tagk: tag name
   */
  static int define_tagk(const std::string& tagk) {
    if (collector_) {
      return collector_->define_tagk(tagk);
    }
    return 1;
  }

  // deprecated
  static int define_tagkv(const std::string& tagk,
                          const std::vector<std::string>& tagv_list) {
    if (collector_) {
      return collector_->define_tagkv(tagk, tagv_list);
    }
    return 1;
  }

  static int define_counter(const std::string& name) {
    if (collector_) {
      return collector_->define_counter(name);
    }
    return 1;
  }

  static int define_counter(const std::string& name,
                            const std::string& /*units*/) {
    if (collector_) {
      return collector_->define_counter(name);
    }
    return 1;
  }

  static int define_rate_counter(const std::string& name) {
    if (collector_) {
      return collector_->define_rate_counter(name);
    }
    return 1;
  }

  static int define_rate_counter(const std::string& name,
                                 const std::string& /*unused*/) {
    if (collector_) {
      return collector_->define_rate_counter(name);
    }
    return 1;
  }

  static int define_meter(const std::string& name) {
    if (collector_) {
      return collector_->define_meter(name);
    }
    return 1;
  }

  static int define_meter(const std::string& name,
                          const std::string& /*unused*/) {
    if (collector_) {
      return collector_->define_meter(name);
    }
    return 1;
  }

  static int define_timer(const std::string& name) {
    if (collector_) {
      return collector_->define_timer(name);
    }
    return 1;
  }

  static int define_timer(const std::string& name,
                          const std::string& /*units*/) {
    if (collector_) {
      return collector_->define_timer(name);
    }
    return 1;
  }

  static int define_store(const std::string& name) {
    if (collector_) {
      return collector_->define_store(name);
    }
    return 1;
  }

  static int define_store(const std::string& name,
                          const std::string& /*units*/) {
    if (collector_) {
      return collector_->define_store(name);
    }
    return 1;
  }

  static int define_ts_store(const std::string& name) {
    if (collector_) {
      return collector_->define_ts_store(name);
    }
    return 1;
  }

  static int define_ts_store(const std::string& name,
                             const std::string& /*units*/) {
    if (collector_) {
      return collector_->define_ts_store(name);
    }
    return 1;
  }

  static int emit_counter(const std::string& name, double value) {
    if (collector_) {
      collector_->emit_counter(name, value);
    }
    return 1;
  }

  static int emit_counter(const std::string& name, double value,
                          const std::string& tagkv) {
    if (collector_) {
      collector_->emit_counter(name, value, tagkv);
    }
    return 1;
  }

  static int emit_counter(const std::string& name, double value,
                          const TagkvList& tagkv_list) {
    if (collector_) {
      collector_->emit_counter(name, value, tagkv_list);
    }
    return 1;
  }

  static int emit_rate_counter(const std::string& name, double value) {
    if (collector_) {
      collector_->emit_rate_counter(name, value);
    }
    return 1;
  }

  static int emit_rate_counter(const std::string& name, double value,
                               const std::string& tagkv) {
    if (collector_) {
      collector_->emit_rate_counter(name, value, tagkv);
    }
    return 1;
  }

  static int emit_rate_counter(const std::string& name, double value,
                               const TagkvList& tagkv_list) {
    if (collector_) {
      collector_->emit_rate_counter(name, value, tagkv_list);
    }
    return 1;
  }

  static int emit_meter(const std::string& name, double value) {
    if (collector_) {
      collector_->emit_meter(name, value);
    }
    return 1;
  }

  static int emit_meter(const std::string& name, double value,
                        const std::string& tagkv) {
    if (collector_) {
      collector_->emit_meter(name, value, tagkv);
    }
    return 1;
  }

  static int emit_meter(const std::string& name, double value,
                        const TagkvList& tagkv_list) {
    if (collector_) {
      collector_->emit_meter(name, value, tagkv_list);
    }
    return 1;
  }

  static int emit_timer(const std::string& name, double value) {
    if (collector_) {
      collector_->emit_timer(name, value);
    }
    return 1;
  }

  static int emit_timer(const std::string& name, double value,
                        const std::string& tagkv) {
    if (collector_) {
      collector_->emit_timer(name, value, tagkv);
    }
    return 1;
  }

  static int emit_timer(const std::string& name, double value,
                        const TagkvList& tagkv_list) {
    if (collector_) {
      collector_->emit_timer(name, value, tagkv_list);
    }
    return 1;
  }

  static int emit_store(const std::string& name, double value) {
    if (collector_) {
      collector_->emit_store(name, value);
    }
    return 1;
  }

  static int emit_store(const std::string& name, double value,
                        const std::string& tagkv) {
    if (collector_) {
      collector_->emit_store(name, value, tagkv);
    }
    return 1;
  }

  static int emit_store(const std::string& name, double value,
                        const TagkvList& tagkv_list) {
    if (collector_) {
      collector_->emit_store(name, value, tagkv_list);
    }
    return 1;
  }

  static int emit_ts_store(const std::string& name, double value, time_t ts) {
    if (collector_) {
      collector_->emit_ts_store(name, value, ts);
    }
    return 1;
  }

  static int emit_ts_store(const std::string& name, double value, time_t ts,
                           const std::string& tagkv) {
    if (collector_) {
      collector_->emit_ts_store(name, value, ts, tagkv);
    }
    return 1;
  }

  static int emit_ts_store(const std::string& name, double value, time_t ts,
                           const TagkvList& tagkv_list) {
    if (collector_) {
      collector_->emit_ts_store(name, value, ts, tagkv_list);
    }
    return 1;
  }

  static int reset_counter(const std::string& name) {
    if (collector_) {
      collector_->reset_counter(name);
    }
    return 1;
  }

  static int reset_counter(const std::string& name, const std::string& tagkv) {
    if (collector_) {
      collector_->reset_counter(name, tagkv);
    }
    return 1;
  }

  static int reset_counter(const std::string& name, double /*value*/,
                           const TagkvList& tagkv_list) {
    if (collector_) {
      collector_->reset_counter(name, tagkv_list);
    }
    return 1;
  }

  static int reset_rate_counter(const std::string& name) {
    if (collector_) {
      collector_->reset_rate_counter(name);
    }
    return 1;
  }

  static int reset_rate_counter(const std::string& name,
                                const std::string& tagkv) {
    if (collector_) {
      collector_->reset_rate_counter(name, tagkv);
    }
    return 1;
  }

  static int reset_rate_counter(const std::string& name, double /*value*/,
                                const TagkvList& tagkv_list) {
    if (collector_) {
      collector_->reset_rate_counter(name, tagkv_list);
    }
    return 1;
  }

  static int reset_timer(const std::string& name) {
    if (collector_) {
      collector_->reset_timer(name);
    }
    return 1;
  }

  static int reset_timer(const std::string& name, const std::string& tagkv) {
    if (collector_) {
      collector_->reset_timer(name, tagkv);
    }
    return 1;
  }

  static int reset_timer(const std::string& name, double /*value*/,
                         const TagkvList& tagkv_list) {
    if (collector_) {
      collector_->reset_timer(name, tagkv_list);
    }
    return 1;
  }

  static int reset_store(const std::string& name) {
    if (collector_) {
      collector_->reset_store(name);
    }
    return 1;
  }

  static int reset_store(const std::string& name, const std::string& tagkv) {
    if (collector_) {
      collector_->reset_store(name, tagkv);
    }
    return 1;
  }

  static int reset_store(const std::string& name, double /*value*/,
                         const TagkvList& tagkv_list) {
    if (collector_) {
      collector_->reset_store(name, tagkv_list);
    }
    return 1;
  }

  static int reset_ts_store(const std::string& name) {
    if (collector_) {
      collector_->reset_ts_store(name);
    }
    return 1;
  }

  static int reset_ts_store(const std::string& name, const std::string& tagkv) {
    if (collector_) {
      collector_->reset_ts_store(name, tagkv);
    }
    return 1;
  }

  static int reset_ts_store(const std::string& name,
                            const TagkvList& tagkv_list) {
    if (collector_) {
      collector_->reset_ts_store(name, tagkv_list);
    }
    return 1;
  }

  // deprecated
  static int start_flush_thread() { return 1; }

  // deprecated
  static int start_listening_thread() { return 1; }

 private:
  static std::unique_ptr<MetricCollector> collector_;
};

} /* namespace metrics2 */
