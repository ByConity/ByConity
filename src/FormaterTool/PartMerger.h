#pragma once
#include <FormaterTool/PartToolkitBase.h>
#include <Poco/Logger.h>

namespace DB
{

using MergeEntryPtr = std::shared_ptr<FutureMergedMutatedPart>;

class PartMerger : public PartToolkitBase
{

public:
    PartMerger(const ASTPtr & query_ptr_, ContextMutablePtr context_);
    void execute() override;

private:

    std::vector<MergeEntryPtr> selectPartsToMerge(const MergeTreeData & data, size_t max_total_size_to_merge, size_t max_total_rows_to_merge);

    void logMergeGroups(const std::vector<MergeEntryPtr> & merge_groups);

    Poco::Logger * log = &Poco::Logger::get("PartMerger");
    String source_path;
    String target_path;
    String working_path;
};

}
