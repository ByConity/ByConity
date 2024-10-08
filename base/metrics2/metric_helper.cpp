#include <metric_helper.h>
#include <cstring>
#include <mutex>
#include <utility>
#include <common/logger_useful.h>

namespace Metrics {

bool isInitialized = false;

std::string tags;
std::once_flag init_metrics_once;

static const std::string EMPTY_VALUE("default");
static std::string getHostIp() {
    std::string value = EMPTY_VALUE;
    if(const char* p = std::getenv("MY_HOST_IP") ){
        value = std::string(p);
        if (!value.empty())
        {
            return value;
        }
    }
    if(const char* p = std::getenv("MY_HOST_IPV6")){
        value = std::string(p);
        if (!value.empty())
        {
            return value;
        }
    }
    if(const char* p = std::getenv("HOST_IP")){
        value = std::string(p);
        if (!value.empty())
        {
            return value;
        }
    }
    if(const char* p = std::getenv("HOST_IPV6")){
        value = std::string(p);
        if (!value.empty())
        {
            return value;
        }
    }
    return value;
}

static std::string getPodNamespace() {
    if(const char* p = std::getenv("MY_POD_NAMESPACE")){
        return std::string(p);
    }
    if(const char* p = std::getenv("POD_NAMESPACE")){
        return std::string(p);
    }
    return EMPTY_VALUE;
}

static std::string getPodName() {
    if(const char* p = std::getenv("MY_POD_NAME")){
        return std::string(p);
    }
    if(const char* p = std::getenv("POD_NAME")){
        return std::string(p);
    }
    return EMPTY_VALUE;
}

std::string TagsToString(const Tags& ts) {
    std::string s;
    for(size_t i = 0; i < ts.size(); i ++) {
        s = s + ts[i].first + '=' + ts[i].second ;
        if(i != ts.size() -1) {
            s = s + '|';
        }
    }
    return s;
}

static void InitMetricsHelper(const metrics2::MetricCollectorConf& config, const std::string & custom_tags) {
    std::string pod_name = getPodName();
    std::string vw_name = EMPTY_VALUE;
    size_t vw_name_pos = pod_name.rfind('-');
    if (vw_name_pos != std::string::npos)
        vw_name = pod_name.substr(0, vw_name_pos);
    Tags commonTags = {{"vws", vw_name}, {"pod", pod_name}, {"namespace", getPodNamespace()}, {"hostip", getHostIp()}};
    tags = TagsToString(commonTags) + custom_tags;
    metrics2::Metrics::init(config);

    isInitialized = true;
    LOG_INFO(getLogger("metric_helper::InitMetricsHelper"), "{}, tag = {}", config.toString(), tags);

    // LOG_INFO(getLogger("metric_helper::InitMetricsHelper"), config.toString() << ", tag = " << tags);
}

void InitMetrics(const metrics2::MetricCollectorConf& config, const std::string & custom_tags) {
    std::call_once(init_metrics_once, InitMetricsHelper, config, custom_tags);
}

void EmitCounter(const std::string& name, double value, const std::string& tag)
{
    if (isInitialized)
    {
        metrics2::Metrics::emit_counter(name, value, tag.empty() ? tags : tags + "|" + tag);
    }
}

void EmitMeter(const std::string& name, double value, const std::string& tag)
{
    if (isInitialized)
    {
        metrics2::Metrics::emit_meter(name, value, tag.empty() ? tags : tags + "|" + tag);
    }
}

void EmitRateCounter(const std::string& name, double value, const std::string& tag)
{
    if (isInitialized)
    {
        metrics2::Metrics::emit_rate_counter(name, value, tag.empty() ? tags : tags + "|" + tag);
    }
}

void EmitStore(const std::string& name, double value, const std::string& tag)
{
    if (isInitialized)
    {
        metrics2::Metrics::emit_store(name, value, tag.empty() ? tags : tags + "|" + tag);
    }
}

void EmitTimer(const std::string& name, double value, const std::string& tag)
{
    if (isInitialized)
    {
        metrics2::Metrics::emit_timer(name, value, tag.empty() ? tags : tags + "|" + tag);
    }
}

void EmitTsStore(const std::string& name, double value, time_t ts, const std::string& tag)
{
    if (isInitialized)
    {
        metrics2::Metrics::emit_ts_store(name, value, ts, tag.empty() ? tags : tags + "|" + tag);
    }
}

void EmitMetric(const MetricType type, const std::string & name, double value, const std::string & tag, time_t ts)
{
    switch (type)
    {
        case Counter:
            EmitCounter(name, value, tag);
            break;
        case Meter:
            EmitMeter(name, value, tag);
            break;
        case Rate:
            EmitRateCounter(name, value, tag);
            break;
        // Store type will show the the last value of 30s sample time
        case Store:
            EmitStore(name, value, tag);
            break;
        case Timer:
            EmitTimer(name, value, tag);
            break;
        case TsStore:
            EmitTsStore(name, value, ts, tag);
            break;
        default:
            break;
    }
}

} // namespace Metrics
