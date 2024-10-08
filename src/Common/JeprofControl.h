#pragma once

#include <Common/Logger.h>
#include <common/singleton.h>

#if __has_include(<Common/config.h>)
    #include <Common/config.h>
#endif

#if USE_JEMALLOC
    #include <atomic>
    #include <Core/Types.h>
    #include <Poco/Logger.h>
    #include <Poco/Util/AbstractConfiguration.h>
#endif

namespace DB
{
class JeprofControl : public ext::singleton<JeprofControl>
{
private:
    bool jemalloc_prof_active {false};
    bool use_mmap_directly {true};

public:
    bool jeprofEnabled() const
    {
        return jemalloc_prof_active;
    }

    bool canUseMmapDirectly() const
    {
        return use_mmap_directly;
    }

#if USE_JEMALLOC
public:
    bool jeprofInitialize(bool enable_by_conf, bool use_mmap_directly);
    void setJeprofStatus(bool active);
    void setProfPath(const String & prof_path_);
    void dump();

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config);
private:
    template<typename T>
    void setJemallocMetric(const String & name, T value);

    String prof_path{"/tmp/"};
    mutable std::atomic<size_t> index{0};
    LoggerPtr log{getLogger("JeprofControl")};
#endif

};

inline bool jeprofEnabled() { return DB::JeprofControl::instance().jeprofEnabled(); }
inline bool canUseMmapDirectly() { return DB::JeprofControl::instance().canUseMmapDirectly(); }
}
