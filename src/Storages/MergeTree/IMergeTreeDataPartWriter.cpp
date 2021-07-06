#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionaryManager.h>

namespace DB
{

Block getBlockAndPermute(const Block & block, const Names & names, const IColumn::Permutation * permutation)
{
    Block result;
    for (size_t i = 0, size = names.size(); i < size; ++i)
    {
        const auto & name = names[i];
        result.insert(i, block.getByName(name));

        /// Reorder primary key columns in advance and add them to `primary_key_columns`.
        if (permutation)
        {
            auto & column = result.getByPosition(i);
            column.column = column.column->permute(*permutation, 0);
        }
    }

    return result;
}

Block permuteBlockIfNeeded(const Block & block, const IColumn::Permutation * permutation)
{
    Block result;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        result.insert(i, block.getByPosition(i));
        if (permutation)
        {
            auto & column = result.getByPosition(i);
            column.column = column.column->permute(*permutation, 0);
        }
    }
    return result;
}

IMergeTreeDataPartWriter::IMergeTreeDataPartWriter(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : data_part(data_part_)
    , storage(data_part_->storage)
    , metadata_snapshot(metadata_snapshot_)
    , columns_list(columns_list_)
    , settings(settings_)
    , index_granularity(index_granularity_)
    , with_final_mark(storage.getSettings()->write_final_mark && settings.can_use_adaptive_granularity)
{
}

Columns IMergeTreeDataPartWriter::releaseIndexColumns()
{
    return Columns(
        std::make_move_iterator(index_columns.begin()),
        std::make_move_iterator(index_columns.end()));
}

void IMergeTreeDataPartWriter::writeImplicitColumnForBitEngine(Block & block)
{
    ColumnsWithTypeAndName columns = block.getColumnsWithTypeAndName();
    ColumnsWithTypeAndName encoded_columns;

    bool need_update = false;

    for (auto & column : columns)
    {
        String name = column.name;

        // For bitengine recode part, column name will end with %20converting
        // so that we need handle this case.
        if (endsWith(column.name, " converting"))
        {
            auto pos = name.rfind(" converting");
            name.replace(pos, 11, "");
        }

        if (!metadata_snapshot->getColumns().hasPhysical(name))
            continue;

        if (!isBitmap64(column.type) || !column.type->isBitEngineEncode())
            continue;

        if (!storage.bitengine_dictionary_manager)
            continue;

        if (!storage.bitengine_dictionary_manager->isValid())
            throw Exception("BitEngine cannot encode column " + name + ", since the dict is invalid", ErrorCodes::LOGICAL_ERROR);

        BitEngineDictionaryPtr dict_ptr;
        if (auto * manager_ptr = dynamic_cast<BitEngineDictionaryManager *>(storage.bitengine_dictionary_manager.get())
                ; manager_ptr)
            dict_ptr = manager_ptr->getBitEngineDictPtr(name);

        if (!dict_ptr)
            continue;

//        size_t bitengine_split_index = storage.getSettings()->bitengine_split_index;
//        String target_idx;
//        if (bitengine_split_index > 0)
//        {
//            if (data_part)
//                target_idx = storage.dataPartPtrToInfo(data_part).getPartitionKeyInStringAt(storage.getSettings()->bitengine_split_index);
//            else
//                throw Exception("Empty DataPart ptr in writeImplicitColumnForBitEngine", ErrorCodes::LOGICAL_ERROR);
//        }
//        size_t dict_idx = 0;
//        if (!target_idx.empty())
//            dict_idx = std::stoll(target_idx);

        try
        {
            ColumnWithTypeAndName encoded_column = dict_ptr->encodeColumn(column, settings.bitengine_settings);
            need_update |= dict_ptr->updated();
            encoded_columns.push_back(encoded_column);
        }
        catch(...)
        {
            if (storage.bitengine_dictionary_manager && need_update)
            {
                storage.bitengine_dictionary_manager->updateVersion();
                storage.bitengine_dictionary_manager->flushDict();
            }
            throw;
        }
    }

    if (!encoded_columns.empty())
    {
        if (settings.bitengine_settings.only_recode)
            block.clear();

        if (storage.bitengine_dictionary_manager && need_update)
        {
            storage.bitengine_dictionary_manager->updateVersion();
            // Do not flush dictionary frequently
            // Only flush it when exception or in destructor
            storage.bitengine_dictionary_manager->lightFlush();
        }

        // TODO (liuhaoqiang) fix how to add a stream for encoded bitmaps
//        auto compression_codec = storage.getContext()->chooseCompressionCodec(0, 0);
        for (auto & encoded_column : encoded_columns)
        {
            block.insertUnique(encoded_column);
        }
    }
}

void IMergeTreeDataPartWriter::updateWriterStream(const NameAndTypePair &pair)
{
    throw Exception("Should implemented in it's sub-class", ErrorCodes::NOT_IMPLEMENTED);
}



IMergeTreeDataPartWriter::~IMergeTreeDataPartWriter() = default;

}
