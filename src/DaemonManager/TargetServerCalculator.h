#pragma once
#include <Interpreters/StorageID.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <CloudServices/CnchServerClient.h>
#include <Interpreters/Context_fwd.h>

namespace DB::DaemonManager
{

class ITargetServerCalculator
{
public:
    virtual CnchServerClientPtr getTargetServer(const StorageID &, UInt64) const = 0;
    virtual ~ITargetServerCalculator() = default;
};

class TargetServerCalculator : public ITargetServerCalculator
{
public:
    TargetServerCalculator(Context & context, CnchBGThreadType type, Poco::Logger * log);
    CnchServerClientPtr getTargetServer(const StorageID &, UInt64) const override;
private:
    CnchServerClientPtr getTargetServerForCnchMergeTree(const StorageID &, UInt64) const;
    CnchServerClientPtr getTargetServerForCnchKafka(const StorageID &, UInt64) const;
    const CnchBGThreadType type;
    Context & context;
    Poco::Logger * log;
};

}
