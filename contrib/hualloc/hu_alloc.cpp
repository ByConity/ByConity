#include "hu_alloc.h"
#include <stdio.h>
#include <vector>
#include <set>
#include <string>
#include <iostream>
#include <sstream>

void* hu_calloc(size_t n, size_t elem_size)
{
    // Overflow check
    const size_t size = n * elem_size;
    if (elem_size != 0 && size / elem_size != n) return nullptr;

    void* result = hu_alloc(size);
    if (result != nullptr)
    {
        memset(result, 0, size);
    }
    return result;
}

void* hu_alloc_aligned(size_t size, size_t align)
{
    if (align > PAGE_SIZE)
        abort();
    return hu_alloc(align > size ? align : size);
}

void* hu_realloc(void* old_ptr, size_t new_size)
{
    if (old_ptr == nullptr)
    {
        void* result = hu_alloc(new_size);
        return result;
    }
    if (new_size == 0)
    {
        hu_free(old_ptr);
        return nullptr;
    }

    void* new_ptr = hu_alloc(new_size);
    if (new_ptr == nullptr)
    {
        return nullptr;
    }

    size_t old_size = hu_getsize(old_ptr);
    memcpy(new_ptr, old_ptr, ((old_size < new_size) ? old_size : new_size));
    hu_free(old_ptr);
    return new_ptr;
}

void hu_free_w(void *p)
{
    hu_free(p);
}

void* hu_alloc_w(size_t sz)
{
    return hu_alloc(sz);
}

void hu_check_init_w()
{
    hu_check_init();
}

void* ReclaimThread(void *args)
{
    // keep & max can be separate for large & segment spaces
    const char * sleep_second = std::getenv("HUALLOC_CLAIM_INTERVAL");
    int sleep = 1;
    try
    {
        if (sleep_second && std::strlen(sleep_second) > 0)
            sleep = atoi(sleep_second);
    }
    catch(...)
    {
        sleep = 1;
    }

    yint cached = *(yint *) args;
    if (sleep > 0)
    {
        for (;;) {
            Sleep(sleep * 1000);
            ui64 total_cached = LargeCached() + SegmentCached();
            if (total_cached < cached * 2)
                continue;

            LargeReclaim(cached, ReclaimMaxReclaim);
            SegmentReclaim(cached, ReclaimMaxReclaim);
        }
    }
}

ui64 LargeCached()
{
    if (AllocatorIsInitialized != 1)
        return 0;
    ui64 total = 0;
    for (yint g = 0; g < LARGE_GROUP_COUNT; ++g)
    {
        TLargeGroupInfo &gg = LargeGroupInfo[g];
        total += _mm_popcnt_u64(gg.FreeBlockMask & gg.CommitedMask);
    }
    return total * LARGE_BLOCK_SIZE;
}

ui64 SegmentCached()
{
    if (AllocatorIsInitialized != 1)
        return 0;
    ui64 total = 0;
    for (yint g = 0; g < SEGMENT_GROUP_COUNT; ++g)
    {
        TSegmentGroupInfo &gg = SegmentGroupInfo[g];
        total += _mm_popcnt_u64(gg.GoodForReclaimMask);
    }

    return total * SEGMENT_SIZE;
}

ui64 HugeAlloc()
{
    return GAllocCnt.load();
}

ui64 LargeReclaimed()
{
    return LargeReclaimCnt.load();
}

ui64 SegmentReclaimed()
{
    return SegmentReclaimCnt.load();
}

/* mbind Policies */
#define MPOL_DEFAULT     0
#define MPOL_PREFERRED   1
#define MPOL_BIND        2
#define MPOL_INTERLEAVE  3
#define MPOL_LOCAL       4
#define MPOL_MAX         5

#define __NR_mbind 237

static long mbind_bytedance(void *start, unsigned long len, int mode,
	const unsigned long *nmask, unsigned long maxnode, unsigned flags)
{
	return syscall(__NR_mbind, (long)start, len, mode, (long)nmask,
				maxnode, flags);
}

bool hualloc_use_numa_info = false;
bool hualloc_enable_mbind = false;
int hualloc_mbind_mode = MPOL_BIND;
void (*hualloc_logger)(std::string) = nullptr;

void hualloc_log(std::string s)
{
    if (hualloc_logger)
        hualloc_logger(s);
    else
        printf(s.c_str());
}

size_t hualloc_numa_node_count = 0;
std::unordered_map<size_t, size_t> hualloc_cpu_index_to_numa_node;

size_t hualloc_used_numa_node_count = 0;
std::unordered_map<size_t, size_t> hualloc_used_numa_nodes_to_mem_index; // node index -> mem index for node

void mbind_memory(char *mem, size_t size, int alignment)
{
    int alignment_count = size/alignment;

    for (auto & hualloc_used_numa_node : hualloc_used_numa_nodes_to_mem_index)
    {
        int mem_index = hualloc_used_numa_node.second;
        int numa_node = hualloc_used_numa_node.first;
        char *mem_cur = mem + (alignment_count/hualloc_numa_node_count) * mem_index * alignment;
        char *mem_next = mem + (alignment_count/hualloc_numa_node_count) * (mem_index+1) * alignment;
        uint64_t mbind_mask = 1ull<<numa_node;

        int res = mbind_bytedance(mem_cur, mem_next-mem_cur, hualloc_mbind_mode, &mbind_mask, hualloc_numa_node_count+1, 0);

        std::stringstream ss;
        ss << "hualloc numa info: bind mem [" << static_cast<void*>(mem_cur) << ", " << static_cast<void*>(mem_next)
           << ") len 0x" << std::hex << mem_next-mem_cur 
           << " @ index " << mem_index << " -> numa node " << numa_node << " return " << res ;
        ss << " err: " << errno << "-" << strerror(errno) << std::endl;
        hualloc_log(ss.str());
    }
}

std::string getCpuListOfNumaNode(int numa_noe)
{
    std::set<int> cpu_set;
    std::string cpu_list;
    for (auto & item : hualloc_cpu_index_to_numa_node)
        if (item.second == numa_noe)
            cpu_set.insert(item.first);
    cpu_list += "[";
    for (auto cpu_index : cpu_set)
        cpu_list += std::to_string(cpu_index) + ",";
    if (!cpu_list.empty()) {
        cpu_list.pop_back();
    }
    cpu_list += "]";
    return cpu_list;
}

void huallocSetNumaInfo(
    size_t max_numa_node_,
    std::vector<cpu_set_t> & numa_nodes_cpu_mask_,
    bool hualloc_enable_mbind_,
    int mbind_mode,
    void (*logger)(std::string)
)
{
    hualloc_logger = logger;
    if (max_numa_node_ <= 0 || numa_nodes_cpu_mask_.size() != max_numa_node_+1)
        return;
    hualloc_enable_mbind = hualloc_enable_mbind_;
    hualloc_numa_node_count = max_numa_node_+1;
    hualloc_mbind_mode = mbind_mode;

    std::stringstream ss;
    ss << "hualloc numa info: max_numa_node: " << max_numa_node_ << ", numa_nodes_cpu_mask.size(): " << numa_nodes_cpu_mask_.size() << std::endl;
    hualloc_log(ss.str());
    for (int i = 0; i < numa_nodes_cpu_mask_.size(); ++i)
    {
        cpu_set_t cpu_mask = numa_nodes_cpu_mask_[i];
        for (int cpu_index = 0; cpu_index < CPU_SETSIZE; ++cpu_index)
        {
            if (CPU_ISSET(cpu_index, &cpu_mask))
            {
                hualloc_cpu_index_to_numa_node[cpu_index] = i;
            }
        }
    }

    cpu_set_t progress_cpu_mask;
    std::set<int> progress_used_numa_nodes;
    CPU_ZERO(&progress_cpu_mask);
    if (sched_getaffinity(0, sizeof(cpu_set_t), &progress_cpu_mask) == -1) {
        hualloc_log("sched_getaffinity fail");
        return;
    }
    for (int cpu_index = 0; cpu_index < CPU_SETSIZE; ++cpu_index)
    {
        if (CPU_ISSET(cpu_index, &progress_cpu_mask))
        {
            progress_used_numa_nodes.insert(hualloc_cpu_index_to_numa_node[cpu_index]);
        }
    }
    hualloc_used_numa_node_count = progress_used_numa_nodes.size();
    if (hualloc_used_numa_node_count <= 0 || hualloc_used_numa_node_count > hualloc_numa_node_count)
    {
        std::stringstream ss;
        ss << "hualloc numa info: hualloc_used_numa_node_count is " << hualloc_used_numa_node_count << ", hualloc_numa_node_count is "
           << hualloc_numa_node_count << ". Won't set hualloc_use_numa_info\n";
        hualloc_log(ss.str());
        return;
    }
    int mem_index = 0;
    for (int hualloc_used_numa_node : progress_used_numa_nodes)
    {
        std::string cpu_list = getCpuListOfNumaNode(hualloc_used_numa_node);
        std::stringstream ss;
        ss << "hualloc numa info: numa node(" << hualloc_used_numa_node << ") -> mem_index(" << mem_index << ") -> cpu list: "
           << cpu_list.c_str() << std::endl;
        hualloc_log(ss.str());

        hualloc_used_numa_nodes_to_mem_index[hualloc_used_numa_node] = mem_index;
        ++mem_index;
    }

    hualloc_use_numa_info = true;
}

int get_thread_numa_mem_index()
{
    int cpu = sched_getcpu();

    int numa_node_index = hualloc_cpu_index_to_numa_node[cpu];
    return hualloc_used_numa_nodes_to_mem_index[numa_node_index];
}

ui64 GetTotalLargeAlloc()
{
    return TotalLargeAlloc.load();
}

ui64 GetTotalLargeFree()
{
    return TotalLargeFree.load();
}

ui64 GetTotalSegmentAlloc()
{
    return TotalSegmentAlloc.load();
}

ui64 GetTotalSegmentFree()
{
    return TotalSegmentFree.load();
}

ui64 GetTotalGiantAlloc()
{
    return TotalGiantAlloc.load();
}

ui64 GetTotalGiantFree()
{
    return TotalGiantFree.load();
}
