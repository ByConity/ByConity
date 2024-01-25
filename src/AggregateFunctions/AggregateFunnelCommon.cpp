#include <AggregateFunctions/AggregateFunnelCommon.h>

namespace DB
{

constexpr size_t PROP_FLAG_SIZE = 64;

// return: prop_flag, level
inline std::pair<UInt64, size_t> getPropFlagFromLevel(const std::vector<UInt64> & prop_flags, size_t current_level)
{
    size_t bucket_index = std::floor(current_level / PROP_FLAG_SIZE);
    size_t flag_index = current_level & (PROP_FLAG_SIZE-1);

    return { prop_flags[bucket_index], flag_index };
}

inline bool nextLevelNeedPropNode(UInt64 prop_flag, size_t current_level)
{
    return (prop_flag >> current_level) & 0x1;
}

inline size_t getExtraPropIndex(UInt64 prop_flag, size_t current_level)
{
    // e.g. prop_flag = 0100 1001, current_level = 3, next level = 4, need to find prop node
    // 2^3 = 8, 8-1 = 0000 0111, 0000 0111 & 0100 1001 = 0000 0001
    // __builtin_popcount(0000 0001) = 1, that is to find the second additional property (extra_prop with index 1)
    return __builtin_popcount(((1 << current_level) - 1) & prop_flag);
}

// Use it only when nextLevelNeedPropNode returns true
bool nextLevelNeedPropNode(const std::vector<UInt64> & prop_flags, size_t current_level)
{
    auto prop_flag = getPropFlagFromLevel(prop_flags, current_level);
    return nextLevelNeedPropNode(prop_flag.first, prop_flag.second);
}

// Use it only when nextLevelNeedPropNode returns true
size_t getExtraPropIndex(const std::vector<UInt64> & prop_flags, size_t current_level)
{
    auto prop_flag = getPropFlagFromLevel(prop_flags, current_level);
    return getExtraPropIndex(prop_flag.first, prop_flag.second);
}

}
