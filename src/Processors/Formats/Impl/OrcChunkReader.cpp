#include "OrcChunkReader.h"
#include <cstddef>
#include <exception>
#include <memory>
#include <Optimizer/PredicateUtils.h>
#include <fmt/core.h>
#include <orc/OrcFile.hh>
#include <orc/Type.hh>
#include <orc/Vector.hh>
#include <orc/sargs/SearchArgument.hh>
#include "Common/Exception.h"
#include "Common/ProfileEvents.h"
#include "common/logger_useful.h"
#include "ArrowBufferedStreams.h"
#include "Columns/ColumnsCommon.h"
#include "Columns/FilterDescription.h"
#include "Core/Block.h"
#include "Core/BlockInfo.h"
#include "Core/Names.h"
#include "Core/Types.h"
#include "DataTypes/DataTypeString.h"
#include "Interpreters/ActionsDAG.h"
#include "Interpreters/Context_fwd.h"
#include "Interpreters/ExpressionActions.h"
#include "Interpreters/IdentifierSemantic.h"
#include "Optimizer/PredicateUtils.h"
#include "Optimizer/SymbolsExtractor.h"
#include "Parsers/IAST_fwd.h"
#include "Parsers/queryToString.h"
#include "Processors/Chunk.h"
#include "Processors/Formats/Impl/OrcCommon.h"
#include "Storages/MergeTree/KeyCondition.h"
#include "Storages/MergeTree/MergeTreeRangeReader.h"
#include "Storages/VirtualColumnUtils.h"
#ifdef CHUNKREADER


namespace ProfileEvents
{
extern const Event OrcSkippedRows;
extern const Event OrcTotalRows;
}
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
    extern const int THERE_IS_NO_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_READ_ALL_DATA;
}

class OrcRowReaderFilter : public orc::RowReaderFilter
{
public:
    OrcRowReaderFilter(OrcChunkReader * chunk_reader_);
    bool filterOnOpeningStripe(uint64_t stripeIndex, const orc::proto::StripeInformation * stripeInformation) override;
    bool filterOnPickRowGroup(
        size_t rowGroupIdx,
        const std::unordered_map<uint64_t, orc::proto::RowIndex> & rowIndexes,
        const std::map<uint32_t, orc::BloomFilterIndex> & bloomFilters) override;
    bool filterMinMax(
        size_t rowGroupIdx,
        const std::unordered_map<uint64_t, orc::proto::RowIndex> & rowIndexes,
        const std::map<uint32_t, orc::BloomFilterIndex> & bloomFilter);

    //sdicts is string column id to string dict.
    bool filterOnPickStringDictionary(const std::unordered_map<uint64_t, orc::StringDictionary *> & sdicts) override;

    bool isColumnEvaluated(size_t orc_column_id) { return dict_filter_cache.find(orc_column_id) != dict_filter_cache.end(); }
    void onStartingPickRowGroups() override;
    void onEndingPickRowGroups() override;
    void setWriterTimezone(const std::string & tz) override;

private:
    // orc column id - > filter mask
    std::map<size_t, ColumnPtr> dict_filter_cache;
    [[maybe_unused]] OrcChunkReader * orc_chunk_reader;
};

OrcRowReaderFilter::OrcRowReaderFilter(OrcChunkReader * chunk_reader_) : orc_chunk_reader(chunk_reader_)
{
}
bool OrcRowReaderFilter::filterOnOpeningStripe(
    [[maybe_unused]] uint64_t stripeIndex, [[maybe_unused]] const orc::proto::StripeInformation * stripeInformation)
{
    return false;
}
bool OrcRowReaderFilter::filterOnPickRowGroup(
    [[maybe_unused]] size_t rowGroupIdx,
    [[maybe_unused]] const std::unordered_map<uint64_t, orc::proto::RowIndex> & rowIndexes,
    [[maybe_unused]] const std::map<uint32_t, orc::BloomFilterIndex> & bloomFilters)
{
    return false;
}
bool OrcRowReaderFilter::filterMinMax(
    [[maybe_unused]] size_t rowGroupIdx,
    [[maybe_unused]] const std::unordered_map<uint64_t, orc::proto::RowIndex> & rowIndexes,
    [[maybe_unused]] const std::map<uint32_t, orc::BloomFilterIndex> & bloomFilter)
{
    return false;
}

// sdict - > string column -> filter by string column
bool OrcRowReaderFilter::filterOnPickStringDictionary([[maybe_unused]] const std::unordered_map<uint64_t, orc::StringDictionary *> & sdicts)
{
    return false;
}


void OrcRowReaderFilter::onStartingPickRowGroups()
{
}
void OrcRowReaderFilter::onEndingPickRowGroups()
{
}
void OrcRowReaderFilter::setWriterTimezone([[maybe_unused]] const std::string & tz)
{
}


OrcScanner::OrcScanner(ScanParams & params) : scan_params(params)
{
}
OrcScanner::~OrcScanner()
{
}

Status OrcScanner::init()
{
    ARROW_RETURN_NOT_OK(prepareFileReader());
    if (file_reader == nullptr)
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "file_reader shall be initialized");
    }
    buildColumnNameToId(scan_params.header, file_reader->getType(), column_name_to_id);
    ARROW_RETURN_NOT_OK(initLazyColumn());
    if (active_indices.empty() || (active_indices.size() + lazy_indices.size() != scan_params.header.columns()))
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "header size does not match");
    }
    ARROW_RETURN_NOT_OK(initChunkReader());
    return Status::OK();
}

// static OrcMemPool scanner_mem_pool;
Status OrcScanner::prepareFileReader()
{
    // prepare file reader.
    std::atomic_int stopped = false;
    auto arrow_file = asArrowFile(*scan_params.in, scan_params.format_settings, stopped, "ORC", ORC_MAGIC_BYTES, /* avoid_buffering */ true);
    orc::ReaderOptions options;
    if (scan_params.orc_tail)
    {
        options.setSerializedFileTail(scan_params.orc_tail.value());
    }


    // options.setMemoryPool(scanner_mem_pool);
    file_reader = orc::createReader(std::make_unique<CachedORCArrowInputStream>(arrow_file), options);
    return Status::OK();
}

/// column name to column id.
/// Note that in Orc there are two id concepts:
/// 1. field id, the index used to getSubType(i).
/// 2. column id, the id assigned in the orc meta. we always choose this.
void OrcScanner::buildColumnNameToId(
    [[maybe_unused]] const Block & header, const orc::Type & root_type, std::map<std::string, int64_t> & column_name_to_id)
{
    // col name to index in orc schema.
    for (size_t i = 0; i < root_type.getSubtypeCount(); ++i)
    {
        std::string name = root_type.getFieldName(i);
        auto col_id = root_type.getSubtype(i)->getColumnId();
        // auto max_col_id = root_type.getSubtype(i)->getMaximumColumnId();
        boost::to_lower(name);
        column_name_to_id[name] = col_id;
        // column_id_to_name[col_id] = name;
    }
}

Status OrcScanner::initChunkReader()
{
    ChunkReaderParams params;
    params.active_indices = active_indices;
    params.active_header = active_header;
    params.lazy_indices = lazy_indices;
    params.lazy_header = lazy_header;
    params.lowcard_indices = lowcard_indices;
    params.lowcardnull_indices = lowcardnull_indices;

    params.file_reader = file_reader.get();
    params.header = scan_params.header;
    params.key_condition = key_condition;
    params.format_settings = scan_params.format_settings;
    params.range_start = scan_params.range_start;
    params.range_length = scan_params.range_length;
    params.read_chunk_size = scan_params.chunk_size;


    chunk_reader = std::make_unique<OrcChunkReader>(params);


    ARROW_RETURN_NOT_OK(chunk_reader->init());
    return Status::OK();
}


Block OrcScanner::mergeBlock(Block & header, Chunk & left, Block & left_header, Chunk & right, Block & right_header)
{
    if (left.getNumColumns() != left_header.columns())
    {
        auto msg = fmt::format("header size didn't match data size, {} /= {} ", left_header.columns(), left.getNumColumns());
        throw Exception(ErrorCodes::LOGICAL_ERROR, msg);
    }
    auto left_block = left_header.cloneEmpty();
    auto columns = left.detachColumns();
    left_block.setColumns(columns);
    return mergeBlock(header, left_block, right, right_header);
}


Block OrcScanner::mergeBlock(Block & header, Block & left_block, Chunk & right, Block & right_header)
{
    if (header.columns() != left_block.columns() + right_header.columns())
    {
        auto msg = fmt::format(
            "header size didn't match in mergeBlock, {} /= {} + {}", header.columns(), left_block.columns(), right_header.columns());
        throw Exception(ErrorCodes::LOGICAL_ERROR, msg);
    }
    // merge left and right into block.
    Block ret = header.cloneEmpty();
    Columns merge;
    merge.resize(ret.columns());

    for (const auto & col : left_block.getColumnsWithTypeAndName())
    {
        auto col_idx = header.getPositionByName(col.name);
        merge[col_idx] = col.column;
    }


    auto right_columns = right.detachColumns();
    int right_idx = 0;
    for (const auto & col_name : right_header.getNames())
    {
        auto col_idx = header.getPositionByName(col_name);
        merge[col_idx] = std::move(right_columns[right_idx]);
        ++right_idx;
    }
    ret.setColumns(merge);
    return ret;
}

// copy from MergeTreeBlockReadUtils
NameSet getRequiredNames(Block & header, const PrewhereInfoPtr prewhere_info)
{
    Names column_names = header.getNames();
    Names pre_column_names;
    if (prewhere_info->alias_actions)
        pre_column_names = prewhere_info->alias_actions->getRequiredColumnsNames();
    else
    {
        pre_column_names = prewhere_info->prewhere_actions->getRequiredColumnsNames();

        if (prewhere_info->row_level_filter)
        {
            NameSet names(pre_column_names.begin(), pre_column_names.end());

            for (auto & name : prewhere_info->row_level_filter->getRequiredColumnsNames())
            {
                if (names.count(name) == 0)
                    pre_column_names.push_back(name);
            }
        }
    }

    // why?
    // if (pre_column_names.empty())
    //     pre_column_names.push_back(column_names[0]);

    NameSet pre_name_set(pre_column_names.begin(), pre_column_names.end());
    return pre_name_set;
}

void attachColumnsToBlock(Chunk & chunk, [[maybe_unused]] Block & chunk_header, Block & block)
{
    assert(chunk_header.columns() == block.columns());
    assert(chunk.getNumColumns() == chunk_header.columns());
    auto columns = chunk.detachColumns();
    block.setColumns(columns);
}

Status OrcScanner::initLazyColumn()
{
    // 1. initialize key_condition
    // 2. initialize active_columns, active_indices, lazy_columns, lazy_indices.
    auto & select_query_info = scan_params.select_query_info;
    Block & header = scan_params.header;
    key_condition = std::make_shared<KeyCondition>(
        select_query_info,
        scan_params.local_context,
        scan_params.header.getNames(),
        std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(header.getColumnsWithTypeAndName())));

    // we actually need to use perwhere to decide lazy columns.
    NameSet key_columns;
    if (select_query_info.prewhere_info)
    {
        key_columns = getRequiredNames(header, select_query_info.prewhere_info);
    }
    auto columns = header.getColumnsWithTypeAndName();
    for (auto it = columns.begin(); it != columns.end(); ++it)
    {
        const auto & col_name = it->name;
        const auto & col_type = it->type;
        auto it_orc_idx = column_name_to_id.find(col_name);
        if (it_orc_idx == column_name_to_id.end())
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "orc file does not contain column {}", col_name);
        }
        auto orc_idx = it_orc_idx->second;
        if (key_columns.contains(col_name))
        {
            active_header.insert(*it);
            active_indices.insert(orc_idx);
        }
        else
        {
            lazy_header.insert(*it);
            lazy_indices.insert(orc_idx);
        }
        if (col_type->lowCardinality() && !col_type->isLowCardinalityNullable())
        {
            lowcard_indices.insert(orc_idx);
        }
        if (col_type->isLowCardinalityNullable())
        {
            lowcardnull_indices.insert(orc_idx);
        }
    }
    if (active_indices.empty())
    {
        std::swap(active_indices, lazy_indices);
        active_header.swap(lazy_header);
    }
    return Status::OK();
}


Status OrcScanner::readNext(Block & block)
{
    for (;;) // do not return empty chunk
    {
        orc::RowReader::ReadPosition position;
        auto status_read_next = chunk_reader->readNext(position);
        ARROW_RETURN_NOT_OK(status_read_next);
        auto next_chunk_ret = chunk_reader->getChunk();
        ARROW_RETURN_NOT_OK(next_chunk_ret);
        Chunk next_chunk(std::move(next_chunk_ret.ValueOrDie()));
        // auto columns = next_chunk.detachColumns();
        Block next_block = active_header.cloneEmpty();
        attachColumnsToBlock(next_chunk, active_header, next_block);
        ProfileEvents::increment(ProfileEvents::OrcTotalRows, next_block.rows());
        // for (int i = 0; i < next_block.rows(); i++)
        // {
        //     LOG_INFO(logger, " row {} {}", i, next_block.getColumns()[0]->get64(i));
        // }
        if (scan_params.select_query_info.prewhere_info == nullptr)
        {
            LOG_INFO(logger, "prewhere is empty in query {}", queryToString(scan_params.select_query_info.query));
            block = std::move(next_block);
            return Status::OK();
        }
        ColumnPtr filter = filterBlock(next_block, scan_params.select_query_info); // this is wrong.
        ConstantFilterDescription constant_filter(*filter);
        FilterDescription filter_desc(*filter);
        uint32_t num_true = countBytesInFilter(*filter_desc.data);
        bool always_true = constant_filter.always_true || num_true == filter->size();
        bool always_false = constant_filter.always_false || num_true == 0;

        if (always_false)
        {
            // block = scan_params.header.cloneEmpty();

            // return Status::OK();
            /// all rows are filtered but we don't return empty block.
            ProfileEvents::increment(ProfileEvents::OrcSkippedRows, filter->size());
            continue;
        }


        if (!always_true)
        {
            for (auto it = next_block.begin(); it != next_block.end(); ++it)
            {
                it->column = it->column->filter(*filter_desc.data, num_true);
            }
        }

        if (lazy_indices.empty())
        {
            block = std::move(next_block);
            return Status::OK();
        }

        // read lazy columns
        ARROW_RETURN_NOT_OK(chunk_reader->lazySeekTo(position.row_in_stripe));
        ARROW_RETURN_NOT_OK(chunk_reader->lazyReadNext(position.num_values));

        // filter on lazy columns

        if (!always_true)
        {
            ARROW_RETURN_NOT_OK(chunk_reader->lazyFilter(filter_desc, num_true));
        }
        auto lazy_chunk_ret = chunk_reader->getLazyChunk();
        ARROW_RETURN_NOT_OK(lazy_chunk_ret);
        // combine active and lazy
        block = mergeBlock(scan_params.header, next_block, lazy_chunk_ret.ValueOrDie(), lazy_header); // this is wrong.
        return Status::OK();
    }
    __builtin_unreachable();
}

ColumnPtr OrcScanner::filterBlock(const Block & block, const SelectQueryInfo & query_info)
{
    auto prewhere_info = query_info.prewhere_info;
    if (!prewhere_info)
        return nullptr;

    std::unique_ptr<PrewhereExprInfo> prewhere_actions;
    {
        prewhere_actions = std::make_unique<PrewhereExprInfo>();
        if (prewhere_info->alias_actions)
            prewhere_actions->alias_actions
                = std::make_shared<ExpressionActions>(prewhere_info->alias_actions, ExpressionActionsSettings{});

        if (prewhere_info->row_level_filter)
            prewhere_actions->row_level_filter
                = std::make_shared<ExpressionActions>(prewhere_info->row_level_filter, ExpressionActionsSettings{});


        prewhere_actions->prewhere_actions
            = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions, ExpressionActionsSettings{});

        prewhere_actions->row_level_column_name = prewhere_info->row_level_column_name;
        prewhere_actions->prewhere_column_name = prewhere_info->prewhere_column_name;
        prewhere_actions->remove_prewhere_column = prewhere_info->remove_prewhere_column;
        prewhere_actions->need_filter = prewhere_info->need_filter;
    }

    size_t num_rows = block.rows();
    Block block_with_filter = block;
    if (prewhere_actions->alias_actions)
        prewhere_actions->alias_actions->execute(block_with_filter, true);
    // TODO what's row_level_filter

    // if (prewhere_info->row_level_filter)
    // {
    //     prewhere_info->row_level_filter->execute(block, true);
    //     block.erase(prewhere_info->row_level_column_name);
    // }

    if (prewhere_actions->prewhere_actions)
        prewhere_actions->prewhere_actions->execute(block_with_filter, &block_with_filter, num_rows, true);

    // if (prewhere_actions->remove_prewhere_column)
    // {
    //     if (block_with_filter.has(prewhere_info->prewhere_column_name))
    //         block_with_filter.erase(prewhere_info->prewhere_column_name);
    // }


    ColumnPtr filter = block_with_filter.getByName(prewhere_info->prewhere_column_name).column->convertToFullColumnIfConst();
    return filter;
}


OrcChunkReader::OrcChunkReader(ChunkReaderParams & params) : chunk_reader_params(params)
{
    key_condition = params.key_condition;
}


Status OrcChunkReader::prepareStripeReader()
{
    // auto arrow_file =
    return Status::OK();
}

OrcChunkReader::~OrcChunkReader()
{
}

Status OrcChunkReader::init()
{
    ARROW_RETURN_NOT_OK(initBlock());
    ARROW_RETURN_NOT_OK(initRowReader());
    return Status::OK();
}

Status OrcChunkReader::initBlock()
{
    active_block = chunk_reader_params.active_header;
    lazy_block = chunk_reader_params.lazy_header;

    auto format_settings = chunk_reader_params.format_settings;
    bool allow_missing_columns = format_settings.orc.allow_missing_columns;
    bool null_as_default = format_settings.null_as_default;
    bool case_insenstive = format_settings.orc.case_insensitive_column_matching;
    bool allow_out_of_range = format_settings.date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Saturate ? true : false;

    //TODO fix this.
    active_orc_column_to_ch_column
        = std::make_unique<ORCColumnToCHColumn>(active_block, allow_missing_columns, null_as_default, case_insenstive, allow_out_of_range);
    lazy_orc_column_to_ch_column
        = std::make_unique<ORCColumnToCHColumn>(lazy_block, allow_missing_columns, null_as_default, case_insenstive, allow_out_of_range);
    orc_column_to_ch_column
        = std::make_unique<ORCColumnToCHColumn>(chunk_reader_params.header, allow_missing_columns, null_as_default, case_insenstive, allow_out_of_range);

    return Status::OK();
}

Status OrcChunkReader::initRowReader()
{
    // key_condition = chunk_reader_params.key_condition;
    auto header = chunk_reader_params.header;
    auto * file_reader = chunk_reader_params.file_reader;
    auto format_settings = chunk_reader_params.format_settings;
    auto current_range_start = chunk_reader_params.range_start;
    auto current_range_length = chunk_reader_params.range_length;

    // search argument.
    {
        auto sarg = buildORCSearchArgument(*key_condition, header, file_reader->getType(), format_settings);
        LOG_DEBUG(logger, "key_condition {}, sargs {}", key_condition->toString(), sarg->toString());
        row_reader_options.searchArgument(std::move(sarg));
        // row_reader_options.rowReaderFilter()
    }

    // various header.
    {
        std::list<uint64_t> included_indices{chunk_reader_params.active_indices.begin(), chunk_reader_params.active_indices.end()};
        included_indices.insert(included_indices.end(), chunk_reader_params.lazy_indices.begin(), chunk_reader_params.lazy_indices.end());
        //TODO(fix the include logic here)
        row_reader_options.includeTypes(included_indices);
        std::list<uint64_t> lazy(chunk_reader_params.lazy_indices.begin(), chunk_reader_params.lazy_indices.end());
        row_reader_options.includeLazyLoadColumnIndexes(lazy);

        row_reader_options.includeLowCardColumnIndexes(
            {chunk_reader_params.lowcard_indices.begin(), chunk_reader_params.lowcard_indices.end()});
        row_reader_options.includeLowCardNullColumnIndexes(
            {chunk_reader_params.lowcardnull_indices.begin(), chunk_reader_params.lowcardnull_indices.end()});
    }

    //only read one stripe.
    {
        row_reader_options.range(current_range_start, current_range_length);
    }

    // the row filter
    {
        auto row_filter = std::make_shared<OrcRowReaderFilter>(this);
        row_reader_options.rowReaderFilter(row_filter);
    }

    row_reader = chunk_reader_params.file_reader->createRowReader(row_reader_options);


    return Status::OK();
}
Status OrcChunkReader::readNext(orc::RowReader::ReadPosition & read_position)
{
    if (!batch)
    {
        batch = row_reader->createRowBatch(chunk_reader_params.read_chunk_size);
        const std::vector<bool> & lazy_selected = row_reader->getLazyLoadColumns();


        // calculate the position of lazy fields in batch
        const auto & selected_root = row_reader->getSelectedType();
        for (size_t i = 0; i < selected_root.getSubtypeCount(); i++)
        {
            auto sub_type = selected_root.getSubtype(i);
            if (lazy_selected[sub_type->getColumnId()])
            {
                lazy_fields.push_back(i);
            }
            else
            {
                active_fields.push_back(i);
            }
        }
    }

    try
    {
        if (!row_reader->next(*batch, &read_position))
        {
            return Status::EndOfFile("");
        }
    }
    catch (std::exception & e)
    {
        LOG_ERROR(logger, "orc read {} failed, error: ", e.what());
        return Status::IOError(fmt::format("orc read {} failed, error: ", e.what()));
    }

    return Status::OK();
}

Result<Chunk> OrcChunkReader::getChunk()
{
    return getActiveChunk();
}

Result<Chunk> OrcChunkReader::getActiveChunk()
{
    Chunk chunk;
    auto numElements = batch->numElements;
    BlockMissingValues missing_values; // useless now.
    const orc::Type & schema = row_reader->getSelectedType();
    // for (int i = 0; i < schema.getSubtypeCount(); i++)
    // {
    //     LOG_INFO(logger, "active {}  {} {}", i, schema.getFieldName(i), schema.getSubtype(i)->toString());
    // }
    // int i = 0;
    // for (const auto b : row_reader->getSelectedColumns())
    // {
    //     if (b)
    //     {
    //         LOG_DEBUG(logger, "selected {} {}", i, b);
    //     }
    //     i++;
    // }
    active_orc_column_to_ch_column->orcTableToCHChunkWithFields(
        active_block, chunk, &schema, batch.get(), active_fields, numElements, &missing_values);
    return chunk;
}

Result<Chunk> OrcChunkReader::getLazyChunk()
{
    Chunk chunk;
    auto numElements = batch->numElements;
    BlockMissingValues missing_values; // useless now.
    // auto lazys = row_reader->getLazyLoadColumns();
    // int i = 0;
    // for (const auto b : lazys)
    // {
    //     if (b)
    //     {
    //         LOG_DEBUG(logger, "lazy {} {}", i, b);
    //     }
    //     i++;
    // }
    const orc::Type & schema = row_reader->getSelectedType();

    // for (int i = 0; i < schema.getSubtypeCount(); i++)
    // {
    //     LOG_INFO(logger, "lazy {}  {} {}", i, schema.getFieldName(i), schema.getSubtype(i)->toString());
    // }
    lazy_orc_column_to_ch_column->orcTableToCHChunkWithFields(
        lazy_block, chunk, &schema, batch.get(), lazy_fields, numElements, &missing_values);
    return chunk;
}

bool OrcChunkReader::useLazyLoad()
{
    return !chunk_reader_params.lazy_indices.empty();
}

Status OrcChunkReader::lazySeekTo(size_t row_in_stripe)
{
    try
    {
        row_reader->lazyLoadSeekTo(row_in_stripe);
    }
    catch (std::exception & e)
    {
        LOG_INFO(logger, "orc lazySeekTo failed, error: {}", e.what());
        return Status::IOError(fmt::format("orc lazySeekTo failed, error: {}", e.what()));
    }
    return Status::OK();
}

Status OrcChunkReader::lazyReadNext(size_t num_values)
{
    try
    {
        row_reader->lazyLoadNext(*batch, num_values);
    }
    catch (std::exception & e)
    {
        LOG_INFO(logger, "orc lazyReadNext {} failed, error: ", e.what());
        return Status::IOError(fmt::format("orc lazyReadNext {} failed, error: ", e.what()));
    }
    return Status::OK();
}

Status OrcChunkReader::lazyFilter(FilterDescription & filter_desc, uint32_t true_size)
{
    batch->filterOnFields(
        reinterpret_cast<uint8_t *>(const_cast<UInt8 *>(filter_desc.data->data())), filter_desc.data->size(), true_size, lazy_fields, true);
    return Status::OK();
}

// TODO(renming): fix
/*
    The implementation here is not correct but enough for now. Modify it.
*/
// Status OrcChunkReader::splitStringFilter(std::map<String, ASTPtr> & string_cond, ASTPtr & rest)
// {
//     auto query_info = chunk_reader_params.select_query_info;
//     auto active_header = chunk_reader_params.active_header;

//     ASTPtr prewhere = query_info.query->getPrewhere();
//     std::vector<ASTPtr> conjuncts = PredicateUtils::extractConjuncts(prewhere);
//     std::map < String, std::vector<ASTPtr> string_conjuncts;
//     std::vector<ASTPtr> rest_conjuncts;

//     for (const auto & conjunct : conjuncts)
//     {
//         auto identifiers = IdentifiersCollector::collect(conjunct);

//         // only extract conjuct with one identifier.
//         if (identifiers.size() != 1)
//         {
//             rest_conjuncts.push_back(conjunct);
//             continue;
//         };

//         auto identifier = identifiers[0];
//         auto name = identifiers->shortName();
//         auto col = active_header->findByName(name);
//         if (col == nullptr)
//         {
//             throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} is not found in header", name);
//         }
//         if (!col->type.isString())
//         {
//             rest_conjuncts.push_back(conjunct);
//             continue;
//         }
//         string_cond[name].push_back()
//     }
//     string_cond = PredicateUtils::combineConjuncts(string_conjuncts);
//     rest = PredicateUtils::combineConjuncts(rest_conjuncts);
// }

// ColumnPtr sdictToStringColumn(orc::StringDictionary & dict)
// {
//     auto str_type = std::make_shared<DataTypeString>();
//     auto internal_column = str_type->createColumn();
//     PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(*internal_column).getChars();
//     PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(*internal_column).getOffsets();
//     auto orc_blob = dict.dictionaryBlob.data();
//     auto orc_num = dict.dictionaryOffset.size() - 1;
//     auto orc_blob_size = dict.dictionaryBlob.size();
//     column_chars_t.reserve(orc_blob_size + orc_num);
//     column_offsets.reserve(orc_num);
//     size_t curr_offset = 0;
//     for (size_t i = 0; i < orc_num; i++)
//     {
//         auto len = dict.dictionaryOffset[i + 1] - dict.dictionaryOffset[i];
//         column_chars_t.insert_assume_reserved(orc_blob, orc_blob + len);
//         column_chars_t.push_back(0);
//         curr_offset += 1 + len;
//         orc_blob = orc_blob + len;
//         column_offsets.push_back(orc_blob);
//     }
//     return internal_column;
// }

// Status OrcChunkReader::evalStringFilterOnDict(
//     ASTPtr & string_cond, std::unordered_map<uint64_t, orc::StringDictionary *> & sdicts, ColumnPtr * dict_filter)
// {
//     // 1. convert sdicts to block.
//     // 2. filter on the block.
//     auto & column_id_to_name = chunk_reader_params.column_id_to_name;
//     auto local_context = chunk_reader_params.local_context;
//     auto active_header = chunk_reader_params.active_header;
//     SelectQueryInfo * select_query_info = chunk_reader_params.select_query_info;


//     Block required_header;
//     Block dict_block;

//     for (auto it = sdicts.begin(); it != sdicts.end(); ++it)
//     {
//         auto col_id = it->first;
//         auto * dict = it->second;
//         auto col = sdictToStringColumn(*dict);
//         dict_block.insert({col, std::make_shared<DataTypeString>(), column_id_to_name[col_id]});
//     }

//     filterBlockWithQuery(select_query_info->query, dict_block, local_context, string_cond);
//     *dict_filter = dict_block.begin()->column;
// }
}
#endif
