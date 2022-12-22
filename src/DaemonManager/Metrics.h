#pragma once

#include <bvar/bvar.h>

namespace DB::DaemonManager::BRPCMetrics
{
    extern bvar::Adder< int > g_executeImpl_PartGC_error;
    extern bvar::Adder< int > g_executeImpl_PartGC;
    extern bvar::Adder< int > g_executeImpl_MergeMutate_error;
    extern bvar::Adder< int > g_executeImpl_MergeMutate;
    extern bvar::Adder< int > g_executeImpl_Consumer_error;
    extern bvar::Adder< int > g_executeImpl_Consumer;
    extern bvar::Adder< int > g_executeImpl_MemoryBuffer_error;
    extern bvar::Adder< int > g_executeImpl_MemoryBuffer;
    extern bvar::Adder< int > g_executeImpl_DedupWorker_error;
    extern bvar::Adder< int > g_executeImpl_DedupWorker;
    extern bvar::Adder< int > g_executeImpl_GlobalGC_error;
    extern bvar::Adder< int > g_executeImpl_GlobalGC;
    extern bvar::Adder< int > g_executeImpl_TxnGC_error;
    extern bvar::Adder< int > g_executeImpl_TxnGC;
    extern bvar::Adder< int > g_executeImpl_Clustering_error;
    extern bvar::Adder< int > g_executeImpl_Clustering;
}/// end namespace

