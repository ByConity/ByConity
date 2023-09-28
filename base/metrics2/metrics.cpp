#include <metrics.h>

#include <memory>

#include <metric_client.h>

namespace metrics2
{

std::unique_ptr<MetricCollector> Metrics::collector_ = nullptr;

int Metrics::init(const MetricCollectorConf& conf) {
  collector_.reset(new MetricCollector());
  return MetricClient::init(conf);
}

} /* namespace metrics2 */
