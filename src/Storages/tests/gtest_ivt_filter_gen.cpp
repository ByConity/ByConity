#include <filesystem>
#include <memory>
#include <gtest/gtest.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include "Columns/IColumn.h"
#include "Core/ColumnWithTypeAndName.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "Core/UUID.h"
#include "Disks/DiskLocal.h"
#include "Interpreters/Context_fwd.h"
#include "Interpreters/DatabaseCatalog.h"
#include "Interpreters/InterpreterCreateQuery.h"
#include "Interpreters/InterpreterDropQuery.h"
#include "Interpreters/InterpreterSelectQuery.h"
#include "Interpreters/InterpreterSelectWithUnionQuery.h"
#include "Interpreters/SelectQueryOptions.h"
#include "Parsers/ASTCreateQuery.h"
#include "Parsers/ParserQuery.h"
#include "Parsers/ParserSelectQuery.h"
#include "Storages/MergeTree/GinIndexDataPartHelper.h"
#include "Storages/MergeTree/GINStoreReader.h"
#include "Storages/MergeTree/GINStoreWriter.h"
#include "Storages/MergeTree/MergeTreeIndexInverted.h"
#include "Storages/MergeTree/MergeTreeIndices.h"

using namespace DB;

namespace
{

ASTPtr parseQuery(const String & query)
{
    const char * begin = query.data();
    const char * end = query.data() + query.size();
    ParserQuery parser(end);
    return parseQuery(parser, begin, end, "", 0, 0);
}

struct IvtFilterGenTest: public testing::Test
{
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
    }

    static void TearDownTestSuite()
    {
        context = nullptr;

        fs::remove_all(store_path);
    }

    static String store_path;
    static ContextMutablePtr context;
};

String IvtFilterGenTest::store_path;
ContextMutablePtr IvtFilterGenTest::context;

struct IvtFilterCtx
{
    IvtFilterCtx(IvtFilterGenTest& suite, const String& create_table_query, const String& query, Block block,
        size_t segment_size = 1024 * 1024 * 1024): context(Context::createCopy(suite.context))
    {
        /// Create test table
        {
            auto create_ast = parseQuery(create_table_query);
            auto* create_table_ast = create_ast->as<ASTCreateQuery>();
            database = create_table_ast->database;
            table = create_table_ast->table;

            if (database.empty() || table.empty())
            {
                throw Exception("Failed to parse database and table from query", ErrorCodes::LOGICAL_ERROR);
            }

            {
                String create_db = fmt::format("create database if not exists {}", database);
                InterpreterCreateQuery interpreter(parseQuery(create_db), context);
                interpreter.execute();
            }

            InterpreterCreateQuery interpreter(parseQuery(create_table_query), context);
            interpreter.execute();
        }

        /// Setup index
        {
            ParserSelectQuery parser;
            auto query_ast = parseQuery(parser, query, 10000, 10000);
            InterpreterSelectQuery interpreter(query_ast, context, SelectQueryOptions());
            auto query_info = interpreter.getQueryInfo();
            query_info.sets = interpreter.getQueryAnalyzer()->getPreparedSets();

            auto storage = DatabaseCatalog::instance().getTable(StorageID(database, table), context);
            idx_desc = storage->getInMemoryMetadataPtr()->getSecondaryIndices();
            if (idx_desc.size() != 1)
            {
                throw Exception("Got multiple index in table", ErrorCodes::LOGICAL_ERROR);
            }
            idx = std::dynamic_pointer_cast<const MergeTreeIndexInverted>(ginIndexCreator(idx_desc[0]));
            idx_cond = std::dynamic_pointer_cast<const MergeTreeConditionInverted>(idx->createIndexCondition(
                query_info, context));
        }

        /// Write index data
        DiskPtr disk = std::make_shared<DiskLocal>("idx_disk", suite.store_path, DiskStats());
        {
            disk->createDirectories(table);
            auto write_store = GINStoreWriter::open(GINStoreVersion::v0, "idx_store",
                std::make_unique<GinDataLocalPartHelper>(disk, table), segment_size, 1.0);

            auto idx_agg = std::dynamic_pointer_cast<MergeTreeIndexAggregatorInverted>(
                idx->createIndexAggregatorForPart(write_store.get()));

            size_t pos = 0;
            idx_agg->update(block, &pos, block.rows());

            write_store->finalize();

            idx_granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleInverted>(idx_agg->getGranuleAndReset());

        }
        /// Create read store
        {
            store = GINStoreReader::open("idx_store", std::make_unique<GinDataLocalPartHelper>(disk, table),
                nullptr);
        }
    }

    ContextMutablePtr context;

    String database;
    String table;

    IndicesDescription idx_desc;
    std::shared_ptr<const MergeTreeIndexInverted> idx;
    std::shared_ptr<const MergeTreeConditionInverted> idx_cond;
    std::shared_ptr<MergeTreeIndexGranuleInverted> idx_granule;

    std::shared_ptr<GINStoreReader> store;
};

void verifyFilter(const roaring::Roaring& filter, const std::vector<size_t>& row_id)
{
    ASSERT_EQ(filter.cardinality(), row_id.size());
    size_t idx = 0;
    for (auto iter = filter.begin(); iter != filter.end(); ++iter, ++idx)
    {
        ASSERT_EQ(row_id[idx], *iter);
    }
}

IvtFilterCtx prepareSingleColEnv(IvtFilterGenTest& suite, const String& table,
    size_t ngram_size, const String& filter_condition, const std::vector<String>& vals)
{
    String create_table_query = fmt::format("create table default.{} (value String, "
        "index val_ivt value type inverted({}) granularity 1) engine = MergeTree "
        "order by tuple()", table, ngram_size);
    String select_query = fmt::format("select * from default.{} where {}",
        table, filter_condition);
    
    MutableColumnPtr col_str = ColumnString::create();
    for (const String& row : vals)
    {
        col_str->insert(Field(row));
    }
    Block block;
    block.insert(ColumnWithTypeAndName(std::move(col_str), std::make_shared<DataTypeString>(),
        "value"));

    return IvtFilterCtx(suite, create_table_query, select_query, block);
}

}

TEST_F(IvtFilterGenTest, equal)
{
    std::vector<String> data = {"ab", "bc", "def", "ghi"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_equal", 2, "value = 'def'", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {2}));
}

TEST_F(IvtFilterGenTest, notEqual)
{
    std::vector<String> data = {"ab", "bc", "def", "ghi"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_not_equal", 2, "value != 'def'", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {0, 1, 3}));
}

TEST_F(IvtFilterGenTest, unknwon)
{
    std::vector<String> data = {"ab", "bc", "def", "ghi"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_unknown", 2, "length(value) == 1", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {0, 1, 2, 3,}));
}

TEST_F(IvtFilterGenTest, like)
{
    std::vector<String> data = {"ab", "bc", "bcd", "bd", "b", "c", "cdb"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_like", 2, "value like '%bc%'", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {1, 2}));
}

TEST_F(IvtFilterGenTest, notlike)
{
    std::vector<String> data = {"ab", "bc", "bcd", "bd", "b", "c", "cdb"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_notlike", 2, "value not like '%bc%'", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {0, 3, 4, 5, 6}));
}

TEST_F(IvtFilterGenTest, hasToken)
{
    std::vector<String> data = {"ab,cd", "de,cd", "fg,hi", "ab", "cd", "cde"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_has_token", 2,
        "hasToken(value, 'cd')", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {0, 1, 4, 5}));
}

TEST_F(IvtFilterGenTest, startsWith)
{
    std::vector<String> data = {"abc", "a", "abc", "bca", "dab", "ab", "000"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_starts_with", 2,
        "startsWith(value, 'ab')", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {0, 2, 4, 5}));
}

TEST_F(IvtFilterGenTest, endWith)
{
    std::vector<String> data = {"abd", "a", "abc", "bca", "def", "cab", "0"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_ends_with", 2,
        "endsWith(value, 'ab')", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {0, 2, 5}));
}

TEST_F(IvtFilterGenTest, multiSearchAny)
{
    std::vector<String> data = {"abc", "cbc", "def", "fabc", "hj"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_multi_search_any", 2,
        "multiSearchAny(value, ['ab', 'bc'])", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {0, 1, 3}));
}

TEST_F(IvtFilterGenTest, in)
{
    std::vector<String> data = {"abc", "cbc", "def", "fabc", "hj"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_in", 2,
        "value in ['ab', 'bc']", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {0, 1, 3}));
}

TEST_F(IvtFilterGenTest, notIn)
{
    std::vector<String> data = {"abc", "cbc", "def", "fabc", "hj"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_not_in", 2,
        "value not in ['ab', 'bc']", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {2, 4}));
}

TEST_F(IvtFilterGenTest, constantTrue)
{
    std::vector<String> data = {"abc", "cbc", "def", "fabc", "hj"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_constant_true", 2,
        "1 = 1", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {0, 1, 2, 3, 4}));
}

TEST_F(IvtFilterGenTest, constantFalse)
{
    std::vector<String> data = {"abc", "cbc", "def", "fabc", "hj"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_constant_false", 2,
        "1 = 0", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {}));
}

TEST_F(IvtFilterGenTest, and)
{
    std::vector<String> data = {"abc", "cbc", "def", "fabc", "hj"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_and", 2,
        "value = 'ab' and value like '%bc%' and (value like '%ab%' and 1)", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {0, 3}));
}

TEST_F(IvtFilterGenTest, or)
{
    std::vector<String> data = {"abc", "cbc", "def", "fabc", "hj"};
    IvtFilterCtx ctx = prepareSingleColEnv(*this, "test_or", 2,
        "value = 'ab' or value like '%bc%' or (value like '%ab%' or 0)", data);

    PostingsCacheForStore cache_store;
    cache_store.store = ctx.store;

    roaring::Roaring filter;
    ctx.idx_cond->mayBeTrueOnGranuleInPart(ctx.idx_granule, cache_store, 0, data.size(), &filter);

    ASSERT_NO_FATAL_FAILURE(verifyFilter(filter, {0, 1, 3}));
}
