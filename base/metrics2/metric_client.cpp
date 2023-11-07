#include <metric_client.h>

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <netdb.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <utility>

#include <msgpack.hpp>

namespace metrics2
{

MetricCollectorConf MetricClient::conf_;
bool MetricClient::initialized_;
sockaddr_un MetricClient::des_addr_;
sockaddr_in MetricClient::des_addr_ip_;
int MetricClient::send_fd_;
std::mutex MetricClient::init_mutex_;

thread_local std::vector<std::vector<std::string>> MetricClient::cmds;
thread_local size_t MetricClient::send_batch_size_ = 0;

static int fill_addrun(struct sockaddr_un* soun, const char* sock_path) {
  memset(soun, 0, sizeof(struct sockaddr_un));
  soun->sun_family = AF_UNIX;
  snprintf(soun->sun_path, sizeof(soun->sun_path), "%s", sock_path);
  return 0;
}

static void reset_destination(struct sockaddr_un &des_addr_, MetricCollectorConf &conf_, std::string &dest) {
  conf_.sock_path.clear();
  conf_.sock_path = dest;
  fill_addrun(&des_addr_, conf_.sock_path.c_str());
}

static void test_connection(struct sockaddr_un &des_addr_, MetricCollectorConf &conf_, int &send_fd_) {
  static std::string initMsg = "CNCH Metric SDK init success.";
  static std::string firstDest = "/opt/tmp/sock/metric.sock";
  static std::string secondDest = "/tmp/metric.sock";
  ssize_t ret = 0;

  ret = sendto(send_fd_, initMsg.c_str(), initMsg.size(), 0,
               reinterpret_cast<sockaddr*>(&(des_addr_)), sizeof(des_addr_));
  if (ret == -1) {
    reset_destination(des_addr_, conf_, secondDest);
    ret = sendto(send_fd_, initMsg.c_str(), initMsg.size(), 0,
                 reinterpret_cast<sockaddr*>(&(des_addr_)), sizeof(des_addr_));
    if (ret == -1) {
        reset_destination(des_addr_, conf_, firstDest);
    }
  }
}

// Tested for following cases:
// 1. s = ".aaa.bbb.ccc..";
// 2. s = "";
// 3. s = "a";
// 4. s = "x.y";
static void inplace_trim_dots(std::string* s) {
  s->erase(0, s->find_first_not_of('.'));
  s->erase(s->find_last_not_of('.') + 1);
}

int MetricClient::init(const MetricCollectorConf& conf) {
  std::lock_guard<std::mutex> lock(init_mutex_);
  if (initialized_) {
    return 0;
  }

  conf_ = conf;
  inplace_trim_dots(&conf_.namespace_prefix);
  if (!conf_.namespace_prefix.empty()) {
    conf_.namespace_prefix += ".";
  }
  int ret = 0;
  if (conf_.use_remote_server) {
      ret = init_with_remote_server();
  } else {
      ret = init_with_domain_socket();
  }
  if (ret < 0) {
      return ret;
  }

  /*
  if (conf_.sock_path.empty()) {
    conf_.sock_path = "/opt/tmp/sock/metric.sock";
  }
  fill_addrun(&des_addr_, conf_.sock_path.c_str());

  send_fd_ = socket(AF_UNIX, SOCK_DGRAM, 0);
  if (send_fd_ < 0) {
    std::cerr << "socket fd error." << std::endl;
    return -1;
  }
  int flags;
  if (-1 == (flags = fcntl(send_fd_, F_GETFL, 0))) {
    std::cerr << "fcntl error." << std::endl;
    return -1;
  }
  int sendBufSize;
  int flag;
  int len = sizeof(sendBufSize);
  setsockopt(send_fd_, SOL_SOCKET, SO_REUSEADDR, &flag, len);
  if (-1 == fcntl(send_fd_, F_SETFL, flags | O_NONBLOCK)) {
    std::cerr << "fcntl error." << std::endl;
    return -1;
  }
  */

  test_connection(des_addr_, conf_, send_fd_);
  initialized_ = true;
  return 0;
}

int MetricClient::init_with_domain_socket() {
    if (conf_.sock_path.empty()) {
        conf_.sock_path = "/tmp/metric.sock";
    }

    send_fd_ = socket(AF_UNIX, SOCK_DGRAM, 0);
    if (send_fd_ < 0) {
        std::cerr << "socket fd error." << std::endl;
        return -1;
    }
    int flags;
    if (-1 == (flags = fcntl(send_fd_, F_GETFL, 0))) {
        std::cerr << "fcntl error." << std::endl;
        return -1;
    }
    int flag = 1;
    int len = sizeof(flag);
    setsockopt(send_fd_, SOL_SOCKET, SO_REUSEADDR, &flag, static_cast<socklen_t>(len));
    // set send timeout
    struct timeval timeout = {0, 20};
    setsockopt(send_fd_, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(struct timeval));
    if (-1 == fcntl(send_fd_, F_SETFL, flags | O_NONBLOCK)) {
        std::cerr << "fcntl error." << std::endl;
        return -1;
    }
    fill_addrun(&des_addr_, conf_.sock_path.c_str());
    return 0;
}

int MetricClient::init_with_remote_server() {
    send_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (send_fd_ < 0) {
        std::cerr << "socket fd error." << std::endl;
        return -1;
    }
    int flags;
    if (-1 == (flags = fcntl(send_fd_, F_GETFL, 0))) {
        std::cerr << "fcntl error." << std::endl;
        return -1;
    }
    int flag = 1;
    int len = sizeof(flag);
    setsockopt(send_fd_, SOL_SOCKET, SO_REUSEADDR, &flag, static_cast<socklen_t>(len));
    // set send timeout
    struct timeval timeout = {0, 20};
    setsockopt(send_fd_, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(struct timeval));
    if (-1 == fcntl(send_fd_, F_SETFL, flags | O_NONBLOCK)) {
        std::cerr << "fcntl error." << std::endl;
        return -1;
    }

    des_addr_ip_.sin_family = AF_INET;
    des_addr_ip_.sin_port = htons(conf_.udp_server_port);

    struct hostent* read_udp_ip = gethostbyname(conf_.udp_server_ip.c_str());
    if (read_udp_ip == nullptr || read_udp_ip->h_addr_list == nullptr ||
        read_udp_ip->h_addr_list[0] == nullptr) {
        std::cerr << "Invalid addr: " << conf_.udp_server_ip << std::endl;
        return -1;
    }
    des_addr_ip_.sin_addr.s_addr = (reinterpret_cast<struct in_addr*>(read_udp_ip->h_addr_list[0]))->s_addr;

    if (inet_pton(AF_INET, conf_.udp_server_ip.c_str(), &des_addr_ip_.sin_addr) < 0) {
        std::cerr << "Invalid ip addr: " << conf_.udp_server_ip << std::endl;
        return -1;
    }

    if (connect(send_fd_, reinterpret_cast<struct sockaddr*>(&des_addr_ip_), sizeof(des_addr_ip_)) < 0) {
        std::cerr << "Failed to connect to metrics server: " << conf_.udp_server_ip << std::endl;
        return -1;
    }
    return 0;
}

int MetricClient::send_message(std::vector<std::string> cmd) {
  cmds.push_back(std::move(cmd));
  if (send_batch_size_ == 0) {
    send_batch_size_ = conf_.send_batch_size;
  }
  if (cmds.size() < send_batch_size_) {
    return 0;
  }
    return flush();
}

int MetricClient::flush() {
  msgpack::sbuffer sbuf;
  msgpack::pack(&sbuf, cmds);
  ssize_t ret = 0;
  if (conf_.use_remote_server) {
    ret = sendto(send_fd_,
                 sbuf.data(),
                 sbuf.size(),
                 0,
                 reinterpret_cast<sockaddr *>(&des_addr_ip_),
                 sizeof(des_addr_ip_));
  } else {
    ret = sendto(send_fd_,
                 sbuf.data(),
                 sbuf.size(),
                 0,
                 reinterpret_cast<sockaddr *>(&des_addr_),
                 sizeof(des_addr_));
  }
  cmds.clear();

  if (ret < static_cast<int>(sbuf.size()) && errno == 90) {
    send_batch_size_ = 1;
  } else if (ret < static_cast<int>(sbuf.size()) && send_batch_size_ < 128 &&
             (conf_.auto_batch == 1)) {
    send_batch_size_ += 1;
    // cerr<<"metric send to metricserver fail, error: "<<strerror(errno)<<endl;
    return -1;
  }
  return 0;
}

} /* namespace metrics2 */
