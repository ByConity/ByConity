#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <mutex>


/// TODO (liuhaoqiang) remove these after all functions are implemented
#pragma  GCC diagnostic ignored  "-Wunused"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-function"


namespace DB
{
class MergeTreeData;

////////////////////////////    Start of IBitEngineDictionaryManager
class IBitEngineDictionaryManager
{
public:
    explicit IBitEngineDictionaryManager(const String & db_tbl_, const String & disk_name_, const String & dict_path_, ContextPtr context_)
        : db_tbl(db_tbl_), disk_name(disk_name_), path(dict_path_), context(context_),shard_id(0)
    { }

    virtual ~IBitEngineDictionaryManager() = default;

    virtual void close() = 0;
    virtual void reload(const String & column_name) = 0;
    virtual void rename(const String & new_db_tbl, const String & new_dict_path) = 0;
    virtual void flushDict() = 0;
    virtual void lightFlush() = 0;

    virtual bool updated() = 0;
    virtual void drop() = 0;
    virtual bool isValid() = 0;
    virtual void setValid() = 0;
    virtual void setInvalid() = 0;

    virtual void updateVersion() = 0;

    virtual void checkBitEnginePart(const MergeTreeData::DataPartPtr & part) const = 0;
    virtual bool hasBitEngineDictionary(const String & name) const = 0;

    virtual std::map<String, UInt64> getAllDictColumnSize() = 0;

    virtual MergeTreeData::MutableDataPartPtr
    encodePartToTemporaryPart(const FutureMergedMutatedPart & future_part,
                              const NamesAndTypesList & columns,
                              const MergeTreeData & merge_tree_data,
                              const ReservationPtr & space_reservation,
                              bool can_skip,
                              bool part_in_detach,
                              bool without_lock) = 0;
    virtual bool recodeBitEnginePart(const FutureMergedMutatedPart & part,
                                     const MergeTreeData & merge_tree_data,
                                     ContextPtr query_context,
                                     bool can_skip,
                                     bool part_in_detach) = 0;
    virtual void recodeBitEngineParts(const std::vector<FutureMergedMutatedPart> & future_parts,
                                      const MergeTreeData & merge_tree_data,
                                      ContextPtr query_context,
                                      bool can_skip,
                                      bool part_in_detach) = 0;
    virtual void recodeBitEnginePartsParallel(const std::vector<FutureMergedMutatedPart> & future_parts,
                                              const MergeTreeData & merge_tree_data,
                                              ContextPtr query_context,
                                              bool can_skip,
                                              bool part_in_detach) = 0;
    virtual bool checkEncodedPart(const MergeTreeData::DataPartPtr & part,
                                  const MergeTreeData & merge_tree_data,
                                  std::unordered_map<String, MergeTreeData::DataPartPtr> & res_abnormal_parts,
                                  bool without_lock) = 0;
    virtual MergeTreeData::DataPartsVector
    checkEncodedParts(const MergeTreeData::DataPartsVector & parts,
                      const MergeTreeData & merge_tree_data,
                      ContextPtr query_context,
                      bool without_lock) = 0;

protected:
    String db_tbl;
    String disk_name;
    String path;
    ContextPtr context;

    String shard_id_macro = "{shard_index}";
    size_t shard_id;

    mutable std::mutex manager_mutex;
    using WriteLock = std::unique_lock<std::mutex>;
    WriteLock getWriteLock() const { return WriteLock(manager_mutex); }
};


////////////////////////////    Start of BitEngineDictionaryManagerBase

template <typename T>
class BitEngineDictionaryManagerBase : public IBitEngineDictionaryManager
{
public:
    BitEngineDictionaryManagerBase(const String & db_tbl_, const String & disk_name_, const String & dict_path_, ContextPtr context_);
    ~BitEngineDictionaryManagerBase() override = default;

    void close() override;
    void lightFlush() override;
    void rename(const String & new_db_tbl, const String & new_dict_path) override;

    bool updated() override;
    void drop() override;
    bool isValid() override;
    void setValid() override;
    void setInvalid() override;

    virtual T getBitEngineDictPtr(const String & column_name) = 0;

    void checkBitEnginePart(const MergeTreeData::DataPartPtr & part) const override;
    bool hasBitEngineDictionary(const String & name) const override { return dict_containers.count(name); }

    bool recodeBitEnginePart(const FutureMergedMutatedPart & part,
                             const MergeTreeData & merge_tree_data,
                             ContextPtr query_context,
                             bool can_skip = false,
                             bool part_in_detach = true) override;
    void recodeBitEngineParts(const std::vector<FutureMergedMutatedPart> & future_parts,
                              const MergeTreeData & merge_tree_data,
                              ContextPtr query_context,
                              bool can_skip = false,
                              bool part_in_detach = true) override;
    void recodeBitEnginePartsParallel(const std::vector<FutureMergedMutatedPart> & future_parts,
                                      const MergeTreeData & merge_tree_data,
                                      ContextPtr query_context,
                                      bool part_in_detach = true,
                                      bool can_skip = false) override;
protected:
    bool dropped = false;

    std::unordered_map<String, T> dict_containers;
};

}
