#pragma once
#include <vector>
#include <llvm/llvm/include/llvm/IR/Module.h>

namespace DB
{
/**
 * Constant Pool for JIT
 */
class JITConstantPool
{
public:
    std::vector<void *> constant_pool;
    ~JITConstantPool()
    {
        for (auto & constant : constant_pool)
            free(constant);
    }
};

/**
 * Context for JIT
 */
class JITContext
{
public:
    JITConstantPool jit_constant_pool;
    llvm::Module * current_module;
};
}
