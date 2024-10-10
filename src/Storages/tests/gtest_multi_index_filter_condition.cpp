#include <filesystem>
#include <unordered_set>
#include <gtest/gtest.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/tests/gtest_global_context.h>
#include "Core/Field.h"
#include "Interpreters/ITokenExtractor.h"
#include "Storages/MergeTree/FilterWithRowUtils.h"
#include "Storages/MergeTree/MarkRange.h"
#include "Storages/MergeTree/MergeTreeIOSettings.h"
#include "Storages/MergeTree/MergeTreeSettings.h"
#include <Core/UUID.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MergeTree/MultiIndexFilterCondition.h>
#include <Storages/IndicesDescription.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/GinIndexDataPartHelper.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

using namespace DB;

namespace
{

const size_t NGRAM_SIZE = 2;

ASTPtr parseQuery(const String & query)
{
    const char * begin = query.data();
    const char * end = query.data() + query.size();
    ParserQuery parser(end);
    return parseQuery(parser, begin, end, "", 0, 0);
}

class MultiIdxFilterTest: public testing::Test
{
public:
    static void SetUpTestSuite()
    {
        tryRegisterFunctions();
        tryRegisterStorages();
        tryRegisterDisks();

        Poco::Util::MapConfiguration* cfg = new Poco::Util::MapConfiguration();
        cfg->setString("additional_services", "");
        cfg->setString("additional_services.FullTextSearch", "1");

        getContext().resetStoragePolicy();
        context = Context::createCopy(getContext().context);
        context->updateAdditionalServices(*cfg);

        store_path = fs::absolute("./" + UUIDHelpers::UUIDToString(UUIDHelpers::generateV4()) + "/");

        fs::create_directories(fs::path(store_path));
        fs::create_directories(fs::path(store_path) / "metastore/");
        fs::create_directories(fs::path(store_path) / "data/");
        fs::create_directories(fs::path(store_path) / "metadata/");

        context->setPath(store_path);
        context->setMetastorePath(store_path + "/metastore/");

        context->setGINStoreReaderFactory({});
    }

    static void TearDownTestSuite()
    {
        context = nullptr;

        fs::remove_all(store_path);
    }

    static String store_path;
    static ContextMutablePtr context;
};

String MultiIdxFilterTest::store_path;
ContextMutablePtr MultiIdxFilterTest::context;

class IdxFilterCtx
{
public:
    IdxFilterCtx(MultiIdxFilterTest& suite_, const String& create_table_query_,
        const String& query_, Block block_): context(Context::createCopy(suite_.context))
    {
        /// Create test table
        {
            auto create_ast = parseQuery(create_table_query_);
            auto* create_table_ast = create_ast->as<ASTCreateQuery>();
            database = create_table_ast->database;
            table = create_table_ast->table;

            if (database.empty() || table.empty())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to parse database {} "
                    "and table {} from query", database, table);
            }

            {
                String create_db = fmt::format("create database if not exists {} engine = Atomic",
                    database);
                InterpreterCreateQuery interpreter(parseQuery(create_db), context);
                interpreter.execute();
            }

            InterpreterCreateQuery interpreter(create_ast, context);
            interpreter.execute();
        }

        StoragePtr storage = DatabaseCatalog::instance().getTable(StorageID(database, table),
            context);
        StorageMergeTree* merge_tree = dynamic_cast<StorageMergeTree*>(storage.get());
        if (merge_tree == nullptr)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Bad storage type");
        }

        /// Write data
        {
            MergeTreeDataWriter writer(*merge_tree);

            StorageMetadataPtr metadata = storage->getInMemoryMetadataPtr();
            BlocksWithPartition blocks_with_partition =
                MergeTreeDataWriter::splitBlockIntoParts(block_, 1, metadata, context);
            for (BlockWithPartition& block_with_partition : blocks_with_partition)
            {
                MergeTreeMetaBase::MutableDataPartPtr temp_part = writer.writeTempPart(
                    block_with_partition, metadata, context);
                merge_tree->renameTempPartAndAdd(temp_part);
            }
        }

        /// Setup index
        {
            ParserSelectQuery parser;
            auto query_ast = parseQuery(parser, query_, 10000, 10000);
            InterpreterSelectQuery interpreter(query_ast, context, SelectQueryOptions());
            query_info = interpreter.getQueryInfo();
            query_info.sets = interpreter.getQueryAnalyzer()->getPreparedSets();

            idx_descs = storage->getInMemoryMetadataPtr()->getSecondaryIndices();
        }

        /// Setup multi index condition
        {
            MergeTreeMetaBase::DataParts data_parts = merge_tree->getDataParts();
            if (data_parts.size() != 1)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Only one part is allowed "
                    " in test");
            }

            part = *(data_parts.begin());
            std::vector<MergeTreeIndexPtr> candidate_indices;
            for (const IndexDescription& index_desc : idx_descs)
            {
                auto index_helper = MergeTreeIndexFactory::instance().get(index_desc);
                candidate_indices.emplace_back(index_helper);
            }
            idx_cond = std::make_unique<MultiIndexFilterCondition>(candidate_indices,
                context, query_info, MergeTreeReaderSettings());
            executor = idx_cond->executor(part);
        }
    }

    std::pair<MarkRanges, std::unique_ptr<roaring::Roaring>> matchEntirePart() const
    {
        IndexTimeWatcher index_timer;
        MarkRanges entire_part_range{MarkRange(0, part->getMarksCount())};
        auto row_filter = executor->match(MarkRanges{MarkRange(0, part->getMarksCount())},
            index_timer);
        MarkRanges ranges = filterMarkRangesByRowFilter(entire_part_range,
            *row_filter, part->index_granularity);
        return {ranges, std::move(row_filter)};
    }

    ContextMutablePtr context;

    String database;
    String table;

    SelectQueryInfo query_info;
    IndicesDescription idx_descs;

    MergeTreeMetaBase::DataPartPtr part;

    std::unique_ptr<MultiIndexFilterCondition> idx_cond;
    std::unique_ptr<MultiIndexFilterCondition::Executor> executor;
};

IdxFilterCtx prepareMultiColEnv(MultiIdxFilterTest& suite_, const String& table_name_,
    const String& filter_condition_, const std::vector<Strings>& vals_, size_t index_granularity_)
{
    size_t col_count = vals_.size();
    String create_table_query = fmt::format("create table multi_idx_ut.{} (", table_name_);
    for (size_t i = 0; i < col_count; ++i)
    {
        if (i != 0)
        {
            create_table_query += ", ";
        }
        create_table_query += fmt::format("v{} String", i);
    }
    for (size_t i = 0; i < col_count; ++i)
    {
        create_table_query += fmt::format(", index v{}_ivt v{} type inverted({}) granularity 1",
            i, i, NGRAM_SIZE);
    }
    create_table_query += fmt::format(") engine = MergeTree order by tuple() settings index_granularity = {}",
        index_granularity_);
    String select_query = fmt::format("select * from multi_idx_ut.{} where {}", table_name_,
        filter_condition_);

    Block block;
    for (size_t i = 0; i < col_count; ++i)
    {
        MutableColumnPtr col_str = ColumnString::create();
        for (const String& row : vals_[i])
        {
            col_str->insert(Field(row));
        }
        block.insert(ColumnWithTypeAndName(std::move(col_str),
            std::make_shared<DataTypeString>(), fmt::format("v{}", i)));
    }
    return IdxFilterCtx(suite_, create_table_query, select_query, block);
}

void verifyFilter(const roaring::Roaring& filter, const std::vector<size_t>& row_id)
{
    ASSERT_EQ(filter.cardinality(), row_id.size());
    size_t idx = 0;
    for (auto iter = filter.begin(); iter != filter.end(); ++iter, ++idx)
    {
        ASSERT_EQ(row_id[idx], *iter);
    }
}

void verifyMarkRanges(const MarkRanges& lhs, const MarkRanges& rhs)
{
    ASSERT_EQ(lhs.size(), rhs.size());
    for (size_t i = 0; i < lhs.size(); ++i)
    {
        ASSERT_EQ(lhs[i].begin, rhs[i].begin);
        ASSERT_EQ(lhs[i].end, rhs[i].end);
    }
}

}

TEST_F(MultiIdxFilterTest, singleColEqual)
{
    std::vector<Strings> data = {
        {"ab", "bc", "def", "ghi"}
    };
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "single_col_eq", "v0 = 'def'", data, 1);
    auto [mark_ranges, row_filter] = ctx.matchEntirePart();
    ASSERT_NO_FATAL_FAILURE(verifyMarkRanges(mark_ranges, MarkRanges{MarkRange(2, 3)}));
    ASSERT_NO_FATAL_FAILURE(verifyFilter(*(row_filter), {2}));
}

TEST_F(MultiIdxFilterTest, singleColNotEqual)
{
    std::vector<Strings> data = {
        {"ab", "bc", "def", "ghi"}
    };
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "single_col_ne", "v0 != 'def'", data, 1);
    auto [mark_ranges, row_filter] = ctx.matchEntirePart();
    ASSERT_NO_FATAL_FAILURE(verifyMarkRanges(mark_ranges, MarkRanges{MarkRange(0, 2), MarkRange(3, 4)}));
    ASSERT_NO_FATAL_FAILURE(verifyFilter(*(row_filter), {0, 1, 3}));
}

TEST_F(MultiIdxFilterTest, singleColUnknown)
{
    std::vector<Strings> data = {
        {"ab", "bc", "def", "ghi"}
    };
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "single_col_unknown", "length(v0) = length('def')", data, 1);
    auto [mark_ranges, row_filter] = ctx.matchEntirePart();
    ASSERT_NO_FATAL_FAILURE(verifyMarkRanges(mark_ranges, MarkRanges{MarkRange(0, ctx.part->getMarksCount())}));
    ASSERT_NO_FATAL_FAILURE(verifyFilter(*(row_filter), {0, 1, 2, 3}));
}

TEST_F(MultiIdxFilterTest, singleColLike)
{
    std::vector<Strings> data = {
        {"ab", "bc", "bcd", "bd", "b", "c", "cdb"}
    };
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "single_col_like", "v0 like '%bc%'", data, 1);
    auto [mark_ranges, row_filter] = ctx.matchEntirePart();
    ASSERT_NO_FATAL_FAILURE(verifyMarkRanges(mark_ranges, MarkRanges{MarkRange(1, 3)}));
    ASSERT_NO_FATAL_FAILURE(verifyFilter(*(row_filter), {1, 2}));
}

TEST_F(MultiIdxFilterTest, singleColNotLike)
{
    std::vector<Strings> data = {
        {"ab", "bc", "bcd", "bd", "b", "c", "cdb"}
    };
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "single_col_not_like", "v0 not like '%bc%'", data, 1);
    auto [mark_ranges, row_filter] = ctx.matchEntirePart();
    ASSERT_NO_FATAL_FAILURE(verifyMarkRanges(mark_ranges, MarkRanges{MarkRange(0, 1), MarkRange(3, 7)}));
    ASSERT_NO_FATAL_FAILURE(verifyFilter(*(row_filter), {0, 3, 4, 5, 6}));
}

TEST_F(MultiIdxFilterTest, singleColMultiSearchAny)
{
    std::vector<Strings> data = {
        {"abc", "cbc", "def", "fabc", "hj"}
    };
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "single_col_multi_search_any", "multiSearchAny(v0, ['ab', 'bc'])",
        data, 1);
    auto [mark_ranges, row_filter] = ctx.matchEntirePart();
    ASSERT_NO_FATAL_FAILURE(verifyMarkRanges(mark_ranges, MarkRanges{MarkRange(0, 2), MarkRange(3, 4)}));
    ASSERT_NO_FATAL_FAILURE(verifyFilter(*(row_filter), {0, 1, 3}));
}

TEST_F(MultiIdxFilterTest, singleColIn)
{
    std::vector<Strings> data = {
        {"abc", "cbc", "def", "fabc", "hj"}
    };
    IndexTimeWatcher index_timer;
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "single_col_in", "v0 in ['ab', 'bc']",
        data, 1);
    auto [mark_ranges, row_filter] = ctx.matchEntirePart();
    ASSERT_NO_FATAL_FAILURE(verifyMarkRanges(mark_ranges, MarkRanges{MarkRange(0, 2), MarkRange(3, 4)}));
    ASSERT_NO_FATAL_FAILURE(verifyFilter(*(row_filter), {0, 1, 3}));
}

TEST_F(MultiIdxFilterTest, singleColNotIn)
{
    std::vector<Strings> data = {
        {"abc", "cbc", "def", "fabc", "hj"}
    };
    IndexTimeWatcher index_timer;
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "single_col_not_in", "v0 not in ['ab', 'bc']",
        data, 1);
    auto [mark_ranges, row_filter] = ctx.matchEntirePart();
    ASSERT_NO_FATAL_FAILURE(verifyMarkRanges(mark_ranges, MarkRanges{MarkRange(2, 3), MarkRange(4, 5)}));
    ASSERT_NO_FATAL_FAILURE(verifyFilter(*(row_filter), {2, 4}));
}

TEST_F(MultiIdxFilterTest, singleColAnd)
{
    std::vector<Strings> data = {
        {"abc", "cbc", "def", "fabc", "hj"}
    };
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "single_col_and",
        "v0 = 'ab' and v0 like '%bc%' and (v0 like '%ab%' and 1)", data, 1);
    auto [mark_ranges, row_filter] = ctx.matchEntirePart();
    ASSERT_NO_FATAL_FAILURE(verifyMarkRanges(mark_ranges, MarkRanges{MarkRange(0, 1), MarkRange(3, 4)}));
    ASSERT_NO_FATAL_FAILURE(verifyFilter(*(row_filter), {0, 3}));
}

TEST_F(MultiIdxFilterTest, singleColOr)
{
    std::vector<Strings> data = {
        {"abc", "cbc", "def", "fabc", "hj"}
    };
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "single_col_or",
        "v0 = 'ab' or v0 like '%bc%' or (v0 like '%ab%' or 0)", data, 1);
    auto [mark_ranges, row_filter] = ctx.matchEntirePart();
    ASSERT_NO_FATAL_FAILURE(verifyMarkRanges(mark_ranges, MarkRanges{MarkRange(0, 2), MarkRange(3, 4)}));
    ASSERT_NO_FATAL_FAILURE(verifyFilter(*(row_filter), {0, 1, 3}));
}

TEST_F(MultiIdxFilterTest, dualColOrOfEquals)
{
    std::vector<Strings> data = {
        {"aa", "ab", "ac", "ad", "ad"},
        {"ba", "bb", "bc", "bd", "be"}
    };
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "dual_col_or_of_eq",
        "v0 = 'ac' or v1 = 'bd'", data, 1);
    auto [mark_ranges, row_filter] = ctx.matchEntirePart();
    ASSERT_NO_FATAL_FAILURE(verifyMarkRanges(mark_ranges, MarkRanges{MarkRange(2, 4)}));
    ASSERT_NO_FATAL_FAILURE(verifyFilter(*(row_filter), {2, 3}));
}

TEST_F(MultiIdxFilterTest, dualColOrInAndEquals)
{
    std::vector<Strings> data = {
        {"aa", "ab", "ac", "ad", "ae"},
        {"ba", "bb", "bc", "bd", "be"},
        {"ca", "cb", "cc", "cd", "ce"}
    };
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "dual_col_or_in_and_equal",
        "v0 = 'ab' or (v1 in ['bc', 'be'] and v2 = 'ce')", data, 2);
    auto [mark_ranges, row_filter] = ctx.matchEntirePart();
    ASSERT_NO_FATAL_FAILURE(verifyMarkRanges(mark_ranges, MarkRanges{MarkRange(0, 1), MarkRange(2, 3)}));
    ASSERT_NO_FATAL_FAILURE(verifyFilter(*(row_filter), {1, 4}));
}

TEST_F(MultiIdxFilterTest, SingleUsefulIndices)
{
    std::vector<Strings> data = {
        {"aa", "ab", "ac", "ad", "ae"},
        {"ba", "bb", "bc", "bd", "be"},
        {"ca", "cb", "cc", "cd", "ce"}
    };
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "single_useful",
        "v1 in ['bc', 'be']", data, 2);
    auto useful_indices = ctx.idx_cond->usefulIndices();
    std::unordered_set<String> useful_indices_set(useful_indices.begin(),
        useful_indices.end());
    ASSERT_EQ(useful_indices_set.size(), 1);
    ASSERT_TRUE(useful_indices_set.contains("v1_ivt"));
}

TEST_F(MultiIdxFilterTest, MultiUsefulIndices)
{
    std::vector<Strings> data = {
        {"aa", "ab", "ac", "ad", "ae"},
        {"ba", "bb", "bc", "bd", "be"},
        {"ca", "cb", "cc", "cd", "ce"}
    };
    IdxFilterCtx ctx = prepareMultiColEnv(*this, "multi_useful",
        "v0 = 'ab' or v1 in ['bc', 'be']", data, 2);
    auto useful_indices = ctx.idx_cond->usefulIndices();
    std::unordered_set<String> useful_indices_set(useful_indices.begin(),
        useful_indices.end());
    ASSERT_EQ(useful_indices_set.size(), 2);
    ASSERT_TRUE(useful_indices_set.contains("v0_ivt"));
    ASSERT_TRUE(useful_indices_set.contains("v1_ivt"));
}
