#include <CloudServices/CnchCreateQueryHelper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Catalog/Catalog.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>

namespace GTEST_Parts_Helper {

class CalcVisibility: public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        tryRegisterStorages();
        tryRegisterDisks();
        getContext().resetStoragePolicy();
    }
};

using namespace DB;

DataModelPartPtr
createPart(const String & partition_id, UInt64 min_block, UInt64 max_block, UInt64 level, UInt64 hint_mutation = 0)
{
    DataModelPartPtr part_model = std::make_shared<Protos::DataModelPart>();
    Protos::DataModelPartInfo * info_model = part_model->mutable_part_info();

    info_model->set_partition_id(partition_id);
    info_model->set_min_block(min_block);
    info_model->set_max_block(max_block);
    info_model->set_level(level);
    info_model->set_hint_mutation(hint_mutation);

    part_model->set_rows_count(0);
    part_model->set_partition_minmax("xxxx");
    part_model->set_marks_count(0);
    part_model->set_size(818);

    return part_model;
}

DB::ServerDataPartPtr createServerDataPart(
    StoragePtr storage, const String & partition_id, size_t min_block, size_t max_block, size_t level, bool deleted, size_t commit_time, size_t hint_mutation)
{
    auto part_model = createPart(partition_id, min_block, max_block, level, hint_mutation);
    part_model->set_deleted(deleted);
    part_model->set_commit_time(commit_time);

    const auto & merge_tree = dynamic_cast<const MergeTreeMetaBase &>(*storage);

    auto part = createPartWrapperFromModel(merge_tree, Protos::DataModelPart(*part_model));
    part->part_model->set_commit_time(commit_time);
    auto ret = std::make_shared<const DB::ServerDataPart>(DB::ServerDataPart(part));
    return ret;
}

void checkParts(ServerDataPartsVector parts, ServerDataPartsVector expected)
{
    sort(parts.begin(), parts.end(), [](const auto & lhs, const auto & rhs) { return lhs->name() < rhs->name(); });
    sort(expected.begin(), expected.end(), [](const auto & lhs, const auto & rhs) { return lhs->name() < rhs->name(); });
    if (parts.size() != expected.size())
    {
        std::cout << "given: " << std::endl;

        for (const auto & part : parts)
        {
            std::cout << part->name() << " deleted? " << toString(part->deleted()) << " commit_time: " << toString(part->get_commit_time())
                      << " previous? " << part->get_info().hint_mutation << std::endl;
        }

        std::cout << "expected: " << std::endl;

        for (const auto & part : expected)
        {
            std::cout << part->name() << " deleted? " << toString(part->deleted()) << std::endl;
        }
    }

    EXPECT_EQ(parts.size(), expected.size());
    for (size_t i = 0; i < parts.size(); i++) {
        EXPECT_EQ(parts[i]->name(), expected[i]->name());
    }
}


void flattenParts(ServerDataPartsVector & parts) {
    for (int i = parts.size() - 1; i >= 0; i--)
    {
        auto cur = parts[i];
        while ((cur = cur->tryGetPreviousPart()))
        {
            parts.push_back(cur);
        }
    }

    sort(parts.begin(), parts.end(), [](const auto & lhs, const auto & rhs) { return lhs->name() < rhs->name(); });
}

TEST_F(CalcVisibility, Basic)
{
    String query = "create table db.test UUID '61f0c404-5cb3-11e7-907b-a6006ad3dba0' (id Int32) ENGINE=CnchMergeTree order by id";
    StoragePtr storage = DB::createStorageFromQuery(query, getContext().context);

    auto merge_tree_meta_base = std::dynamic_pointer_cast<DB::MergeTreeMetaBase>(storage);
    ASSERT_TRUE(merge_tree_meta_base != nullptr);

    auto p = [&](String partition_id,
                 size_t min_block,
                 size_t max_block,
                 size_t level,
                 bool deleted,
                 size_t commit_time,
                 size_t hint_mutation = 0) {
        return createServerDataPart(storage, partition_id, min_block, max_block, level, deleted, commit_time, hint_mutation);
    };

    {
        std::cout << "parts" << std::endl;
        // P <- Partial
        // P
        // P
        ServerDataPartsVector origin = {
            p("20230101", 1, 1, 0, false, 111),
            /// Partial part has higher level and commit_time.
            p("20230101", 1, 1, 1, false, 123, 111),
            p("20230101", 2, 2, 0, false, 222),
            p("20240101", 1, 1, 0, false, 123),
        };


        ServerDataPartsVector invisible;
        ServerDataPartsVector visible;
        visible = calcVisibleParts(origin, false, CnchPartsHelper::LoggingOption::DisableLogging, &invisible);

        flattenParts(visible);
        flattenParts(invisible);

        checkParts(visible, origin);
        checkParts(invisible, {});
    }

    {
        std::cout << "drop range" << std::endl;
        // P ◄─┬─ DropRange
        // P ◄─┤
        // P ◄─┘
        // P
        // ---
        // DropRange
        ServerDataPartsVector origin = {
            p("20230101", 1, 1, 0, false, 112),
            p("20230101", 2, 2, 0, false, 222),
            p("20230101", 4, 8, 3, false, 234),
            p("20230101", 0, 10, MergeTreePartInfo::MAX_LEVEL, true, 235),
            p("20230101", 11, 11, 1, false, 236),
            p("20240101", 0, 10, MergeTreePartInfo::MAX_LEVEL, true, 235),
        };

        ServerDataPartsVector invisible;
        ServerDataPartsVector visible;
        visible = calcVisibleParts(origin, false, CnchPartsHelper::LoggingOption::DisableLogging, &invisible);

        flattenParts(visible);
        flattenParts(invisible);

        checkParts(visible, {
            p("20230101", 11, 11, 1, false, 235),
        });
        checkParts(invisible, {
            p("20230101", 1, 1, 0, false, 112),
            p("20230101", 2, 2, 0, false, 222),
            p("20230101", 4, 8, 3, false, 234),
            p("20230101", 0, 10, MergeTreePartInfo::MAX_LEVEL, true, 235),
            p("20240101", 0, 10, MergeTreePartInfo::MAX_LEVEL, true, 235),
        });
    }

    {
        std::cout << "dropped part" << std::endl;
        // P ◄─ Dropped
        // P ◄─ Dropped
        ServerDataPartsVector origin = {
            p("20230101", 1, 1, 0, false, 111),
            p("20230101", 1, 1, 1, true, 222),
            p("20230101", 2, 2, 0, false, 111),
            p("20230101", 2, 2, 1, true, 222),
        };

        ServerDataPartsVector invisible;
        ServerDataPartsVector visible;
        visible = calcVisibleParts(origin, false, CnchPartsHelper::LoggingOption::DisableLogging, &invisible);

        flattenParts(invisible);
        flattenParts(visible);

        checkParts(visible, {});
        checkParts(invisible, origin);
    }


    {
        std::cout << "dropped part with merge" << std::endl;
        // P ◄─ Dropped ◄─┬─ P
        // P ◄─ Dropped ◄─┘
        ServerDataPartsVector origin = {
            p("20230101", 1, 1, 0, false, 111),
            p("20230101", 1, 1, 1, true, 222),
            p("20230101", 2, 2, 0, false, 111),
            p("20230101", 2, 2, 1, true, 222),
            p("20230101", 1, 2, 1, false, 222),
        };

        ServerDataPartsVector invisible;
        ServerDataPartsVector visible;
        visible = calcVisibleParts(origin, false, CnchPartsHelper::LoggingOption::DisableLogging, &invisible);

        flattenParts(visible);
        flattenParts(invisible);
        checkParts(
            visible,
            {
                p( "20230101", 1, 2, 1, false, 222),
            });
        checkParts(
            invisible,
            {
                p("20230101", 1, 1, 0, false, 111),
                p("20230101", 1, 1, 1, true, 222),
                p("20230101", 2, 2, 0, false, 111),
                p("20230101", 2, 2, 1, true, 222),
            });
    }

}

}
