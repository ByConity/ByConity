#pragma once
#include <string>

/* format of shared memory file name */
static inline void shm_file_fmt(char *buf, uint64_t prefix, uint16_t idx)
{
    sprintf(buf, "%lx.%x", prefix, idx);
}

/* format of RPC path of Unix Domain Socket */
static inline void rpc_uds_fmt(char *buf, const char *uds_path)
{
    sprintf(buf, "unix:%s", uds_path);
}

