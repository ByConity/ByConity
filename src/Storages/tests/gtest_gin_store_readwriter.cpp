#include <filesystem>
#include <random>
#include <unordered_map>
#include <unistd.h>
#include <gtest/gtest.h>
#include <Disks/DiskLocal.h>
#include <Storages/MergeTree/GINStoreReader.h>
#include <Storages/MergeTree/GINStoreWriter.h>
#include "common/types.h"
#include "Core/UUID.h"
#include "Storages/MergeTree/GINStoreCommon.h"
#include "Storages/MergeTree/GinIndexDataPartHelper.h"

using namespace DB;

namespace
{

class GINStoreV0V1V2Test: public testing::Test
{
public:

    static void SetUpTestSuite()
    {
        store_path = "./" + UUIDHelpers::UUIDToString(UUIDHelpers::generateV4()) + "/";
        fs::create_directories(store_path);
    }

    static void TearDownTestSuite()
    {
        fs::remove_all(store_path);
    }

    static String store_path;
};

String GINStoreV0V1V2Test::store_path;

struct TermPostings
{
    std::unordered_map<UInt32, std::set<UInt32>> segment_rows;
};

void verifyPostings(const std::unordered_map<String, TermPostings>& actual_term_postings_,
    const GinPostingsCache& postings_)
{
    ASSERT_EQ(actual_term_postings_.size(), postings_.size());

    for (const auto& term_and_postings : actual_term_postings_)
    {
        ASSERT_TRUE(postings_.contains(term_and_postings.first));

        const auto& store_segment_postings = postings_.at(term_and_postings.first);
        for (const auto& [segment_id, segment_rows] : term_and_postings.second.segment_rows)
        {
            if (segment_rows.empty())
            {
                ASSERT_TRUE(!store_segment_postings.contains(segment_id)
                    || store_segment_postings.at(segment_id)->isEmpty());
            }
            else
            {
                ASSERT_TRUE(store_segment_postings.contains(segment_id));
                const auto& store_posting_list = store_segment_postings.at(segment_id);
                ASSERT_EQ(segment_rows.size(), store_posting_list->cardinality());
                for (const auto& row_id : segment_rows)
                {
                    ASSERT_TRUE(store_posting_list->contains(row_id));
                }
            }
        }
    }
}

}

TEST_F(GINStoreV0V1V2Test, V0ReadWrite)
{
    DiskPtr disk = std::make_shared<DiskLocal>("v0_test_disk", store_path, DiskStats {});
    String idx_dir = "multi_seg_v0";
    String idx_name = "index";
    disk->createDirectories(idx_dir);

    std::unordered_map<String, TermPostings> term_postings;
    {
        auto gin_store_helper = std::make_unique<GinDataLocalPartHelper>(disk, idx_dir);
        auto write_store = GINStoreWriter::open(GINStoreVersion::v0, idx_name,
            std::move(gin_store_helper), 100, 1.0);

        ASSERT_EQ(write_store->segmentID(), 0);
        ASSERT_EQ(write_store->rowID(), 0);

        std::default_random_engine re;
        std::uniform_int_distribution<int> dist(0, 100);
        size_t segment_id = 0;
        for (size_t row = 0; row < 100; ++row)
        {
            for (size_t i = 0; i < 10; ++i)
            {
                String term = fmt::format("term_{}", dist(re));
                term_postings[term].segment_rows[segment_id].insert(row);
                write_store->appendPostingEntry(term, row);
            }

            if (row != 0 && row % 30 == 0)
            {
                ++segment_id;
                write_store->writeSegment();
            }
        }
        write_store->finalize();
    }

    {
        auto gin_store_helper = std::make_unique<GinDataLocalPartHelper>(disk, idx_dir);
        auto read_store = GINStoreReader::open(idx_name, std::move(gin_store_helper),
            nullptr);

        std::set<String> terms;
        for (const auto& entry : term_postings)
        {
            terms.insert(entry.first);
        }
        GinPostingsCachePtr postings = read_store->createPostingsCacheFromTerms(terms);
        ASSERT_NO_FATAL_FAILURE(verifyPostings(term_postings, *postings));
    }
}

TEST_F(GINStoreV0V1V2Test, V2ReadWrite)
{
    DiskPtr disk = std::make_shared<DiskLocal>("v2_test_disk", store_path, DiskStats {});
    String idx_dir = "multi_seg_v2";
    String idx_name = "index";
    disk->createDirectories(idx_dir);

    std::unordered_map<String, TermPostings> term_postings;
    {
        auto gin_store_helper = std::make_unique<GinDataLocalPartHelper>(disk, idx_dir);
        auto write_store = GINStoreWriter::open(GINStoreVersion::v2, idx_name,
            std::move(gin_store_helper), 100, 1.0);

        ASSERT_EQ(write_store->segmentID(), 0);
        ASSERT_EQ(write_store->rowID(), 0);

        std::default_random_engine re;
        std::uniform_int_distribution<int> dist(0, 100);
        size_t segment_id = 0;
        for (size_t row = 0; row < 100; ++row)
        {
            for (size_t i = 0; i < 10; ++i)
            {
                String term = fmt::format("term_{}", dist(re));
                term_postings[term].segment_rows[segment_id].insert(row);
                write_store->appendPostingEntry(term, row);
            }

            if (row != 0 && row % 30 == 0)
            {
                ++segment_id;
                write_store->writeSegment();
            }
        }
        write_store->finalize();
    }

    {
        auto gin_store_helper = std::make_unique<GinDataLocalPartHelper>(disk, idx_dir);
        auto read_store = GINStoreReader::open(idx_name, std::move(gin_store_helper),
            nullptr);

        std::set<String> terms;
        for (const auto& entry : term_postings)
        {
            terms.insert(entry.first);
        }
        GinPostingsCachePtr postings = read_store->createPostingsCacheFromTerms(terms);
        ASSERT_NO_FATAL_FAILURE(verifyPostings(term_postings, *postings));
    }
}
