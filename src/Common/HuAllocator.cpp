#include "HuAllocator.h"

#if USE_HUALLOC
template class HuAllocator<false>;
template class HuAllocator<true>;
#endif
