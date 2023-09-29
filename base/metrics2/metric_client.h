#pragma once

#include <sys/un.h>
#include <netinet/in.h>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <metric_collector.h>
#include <metric_collector_conf.h>

namespace metrics2
{

class MetricClient {
 public:
  MetricClient() = delete;
  ~MetricClient() = delete;

  
  static int init(const MetricCollectorConf& conf);

  // TODO:
  static int send_emit_message(const std::string& action,
                               const std::string& type, const std::string& name,
                               double value, std::string tagkv,
                               bool use_global_prefix = true, time_t ts = -1) {
    if (!initialized_) {
      return 1;
    }
    if (ts == -1) {
      return send_message(
          {action, type, get_canonical_metric_name(name, use_global_prefix),
           std::to_string(value), append_psm(std::move(tagkv)), ""});
    }
    return send_message({action, type,
                         get_canonical_metric_name(name, use_global_prefix),
                         std::to_string(value), append_psm(std::move(tagkv)),
                         std::to_string(ts)});
  }

  // TODO:
  static int send_reset_message(const std::string& action,
                                const std::string& type,
                                const std::string& name, double value,
                                std::string tagkv,
                                bool use_global_prefix = true) {
    if (!initialized_) {
      return 1;
    }
    // 不知道为什么这里少一个空字符串字段
    return send_message({action, type,
                         get_canonical_metric_name(name, use_global_prefix),
                         std::to_string(value), append_psm(std::move(tagkv))});
  }

  static int flush();

  static std::string get_canonical_metric_name(const std::string& name,
                                               bool use_global_prefix = true) {
    if (!use_global_prefix) {
      return name;
    }
    return conf_.namespace_prefix + name;
  }

 private:
  static std::string append_psm(std::string tagkv) {
    if (!conf_.psm.empty()) {
      if (tagkv.empty()) {
        tagkv.append("_psm=");
        tagkv.append(conf_.psm);
      } else {
        tagkv.append("|_psm=");
        tagkv.append(conf_.psm);
      }
    }
    return tagkv;  // Copy-elision instead of std::move.
  }

  static int send_message(std::vector<std::string> cmd);

  static int init_with_domain_socket();
  static int init_with_remote_server();

  static MetricCollectorConf conf_;
  static bool initialized_;
  static sockaddr_un des_addr_;
  static sockaddr_in des_addr_ip_;
  static int send_fd_;
  static std::mutex init_mutex_;

  static thread_local std::vector<std::vector<std::string>> cmds;
  static thread_local size_t send_batch_size_;
};

} /* namespace metrics2 */
