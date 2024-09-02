#include <Common/JeprofControl.h>

#if USE_JEMALLOC
#    include <atomic>
#    include <IO/WriteBufferFromFile.h>
#    include <IO/WriteHelpers.h>
#    include <fmt/format.h>
#    include <jemalloc/jemalloc.h>
#    include <Poco/Logger.h>
#    include <Common/Exception.h>
#    include <Common/MemoryTracker.h>
#    include <common/logger_useful.h>
#    include <cstdlib>
#endif


namespace DB
{
#if USE_JEMALLOC

static constexpr Int64 SMALL_MEMORY_THRESHOLD = 107374182400;

template<typename T>
void JeprofControl::setJemallocMetric(const String & name, T value)
{
    T old_value;
    size_t length = sizeof(old_value);
    if (mallctl(name.c_str(), &old_value, &length, &value, sizeof(T)))
        throw Poco::SystemException("mallctl() failed for " + name); 

    if (old_value != value)
        LOG_INFO(log, "change '" + name + "' from " + std::to_string(old_value) + " to " + std::to_string(value)); 
}

template<typename T>
static void getJemallocMetric(const String & name, T & value)
{
    size_t length = sizeof(value);
    if (mallctl(name.c_str(), &value, &length, nullptr, 0))
        throw Poco::SystemException("mallctl() failed for " + name); 
}

bool JeprofControl::jeprofInitialize(bool enable_by_conf, bool use_mmap_directly_)
{
    use_mmap_directly = use_mmap_directly_;
    if (enable_by_conf)
    {
        setJeprofStatus(true);
        LOG_INFO(log, "enable jeprof through configuration file");
    }
    else if (getenv("MALLOC_CONF"))
    {
        bool active;
        getJemallocMetric("prof.active", active);
        jemalloc_prof_active = active;
    }
    return jemalloc_prof_active;
}

void JeprofControl::setJeprofStatus([[maybe_unused]] bool active)
{
    setJemallocMetric("prof.active", active);
    jemalloc_prof_active = active;
}

void JeprofControl::setProfPath(const String & prof_path_)
{
    prof_path = prof_path_;
}

static void dump_stats_cb(void * wb, const char * buf)
{
    DB::WriteBuffer * write_buffer = static_cast<DB::WriteBuffer *>(wb);
    DB::writeStringBinary(buf, *write_buffer);
}

void JeprofControl::dump()
{
    if (!jeprofEnabled())
        return;

    size_t i = index.fetch_add(1, std::memory_order_relaxed);
    String stats_filename = fmt::format("{}jemalloc_stats_{}.{}.stats", prof_path, getpid(), i);
    WriteBufferFromFile wb(stats_filename);
    malloc_stats_print(dump_stats_cb, &wb, nullptr);
    wb.close();
    LOG_DEBUG(log, "jeprof dumped stats file: " + stats_filename);

    String prof_filename = fmt::format("{}jemalloc_prof_{}.{}.heap", prof_path, getpid(), i);
    const char * prof_file = prof_filename.c_str();
    mallctl("prof.dump", nullptr, nullptr, &prof_file, sizeof(const char *));
    LOG_DEBUG(log, "jeprof dumped prof file: " + prof_filename);
}
void JeprofControl::loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (config.has("jemalloc.background_thread"))
    {
        bool background_thread = config.getBool("jemalloc.background_thread");
        setJemallocMetric("background_thread", background_thread);
    }
    else
    {
        bool current_background_thread;
        getJemallocMetric("background_thread", current_background_thread);
        bool background_thread{false};
        String message;
        /// Enable background_thread when using jeprof to reduce the dirty memory
        bool active;
        getJemallocMetric("prof.active", active);
        if (active)
        {
            background_thread = true;
            message = "enable background_thread due to jeprof enabled";
        }

        /// Enable background_thread when clickhouse runs with small memory
        auto max_server_memory_usage = total_memory_tracker.getHardLimit();
        if (max_server_memory_usage < SMALL_MEMORY_THRESHOLD)
        {
            background_thread = true;
            message = fmt::format("enable background_thread due to small memory, max_server_memory_usage {}, threshold {}"
                , max_server_memory_usage, SMALL_MEMORY_THRESHOLD);
        }
        else
        {
            if (!background_thread)
                message = fmt::format("disable background_thread due to large memory, max_server_memory_usage {}, threshold {}"
                    , max_server_memory_usage, SMALL_MEMORY_THRESHOLD);
        }

        if (background_thread != current_background_thread)
        {
            setJemallocMetric("background_thread", background_thread);
            LOG_INFO(log, message);
        }
    }
    auto setArena = [&config, this] (const String & name)
    {
        auto config_name = "jemalloc." + name;
        if (config.has(config_name))
        {
            ssize_t value = config.getInt(config_name);
            setJemallocMetric("arenas." + name, value); 
        }        
    };

    setArena("dirty_decay_ms");
    setArena("muzzy_decay_ms");
}

#endif
}
