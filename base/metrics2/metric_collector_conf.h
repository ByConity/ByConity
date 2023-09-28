#pragma once

#include <string>

namespace metrics2
{

/**

 *  metrics_enabled_backends: ganglia,file
 *
 *  metrics_backend_opentsdb_endpoints: 192.168.20.41:8400
 * 
 *  metrics_flush_interval: 10s
 *
 *  metrics_namespace_prefix: bytedance.recommend.sort
 */
class MetricCollectorConf {
 public:
  std::string namespace_prefix;
  std::string udp_server_ip;
  int udp_server_port;
  std::string sock_path;
  size_t send_batch_size;
  int auto_batch;
  std::string psm;
  bool enable_debug_metric;
  bool use_remote_server;

  MetricCollectorConf()
      : udp_server_ip("127.0.0.1"),
        udp_server_port(9123),
        send_batch_size(1),
        auto_batch(1),
        enable_debug_metric(false) {
    char* c_psm = std::getenv("LOAD_SERVICE_PSM");
    if (c_psm != nullptr) {
      psm = std::string(c_psm);
    }
  }

  std::string toString() const
  {
      return "namespace_prefix: " + namespace_prefix + ", " +
                  "udp_server_ip: " + udp_server_ip + ", " +
                  "udp_server_port: " + std::to_string(udp_server_port) + ", " +
                  "sock_path: " + sock_path + ", " +
                  "send_batch_size: " + std::to_string(send_batch_size) + ", " +
                  "auto_batch: " + std::to_string(auto_batch) + ", " +
                  "psm: " + psm + ", " +
                  "enable_debug_metric: " + (enable_debug_metric == true ? "true" : "false") + ", " +
                  "use_remote_server: " + (use_remote_server == true ? "true" : "false");
  }
};

} /* namespace metrics2 */
