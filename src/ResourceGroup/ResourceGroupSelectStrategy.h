#pragma once

#include <memory>
#include <Parsers/IAST.h>
#include <Poco/Logger.h>
#include <Interpreters/Context.h>

namespace DB
{

class IResourceGroupManager;
class IResourceGroup;

class IResouceGroupSelectStrategy
{
public:
    IResouceGroupSelectStrategy(IResourceGroupManager * manager)
    {
        resource_group_manager = manager;
    }
    virtual ~IResouceGroupSelectStrategy()
    {
    }

    virtual IResourceGroup* selectGroup(const Context & query_context, const IAST *ast) = 0;

protected:
    IResourceGroupManager * resource_group_manager;
    Poco::Logger* logger;
};

class UserTableSelectStrategy : public IResouceGroupSelectStrategy
{
public:
    UserTableSelectStrategy(IResourceGroupManager * manager)
        : IResouceGroupSelectStrategy(manager)
    {
        logger = &Poco::Logger::get("UserTableSelectStrategy");
    }
    ~UserTableSelectStrategy() override
    {

    }

public:
    IResourceGroup* selectGroup(const Context & query_context, const IAST *ast) override;
};

class UserQuerySelectStrategy: public IResouceGroupSelectStrategy
{
public:
    UserQuerySelectStrategy(IResourceGroupManager * manager) 
        : IResouceGroupSelectStrategy(manager)
    {
        logger = &Poco::Logger::get("UserQuerySelectStrategy");
    }
    ~UserQuerySelectStrategy() override
    {

    }

public:
    IResourceGroup* selectGroup(const Context & query_context, const IAST *ast) override;
};

class VWSelectStrategy: public IResouceGroupSelectStrategy
{
public:
    VWSelectStrategy(IResourceGroupManager * manager) 
        : IResouceGroupSelectStrategy(manager)
    {
        logger = &Poco::Logger::get("VWSelectStrategy");
    }
    ~VWSelectStrategy() override
    {
    }

public:
    IResourceGroup* selectGroup(const Context & query_context, const IAST *ast) override;
};

}
