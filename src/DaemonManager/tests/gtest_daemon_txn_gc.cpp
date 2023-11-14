#include <DaemonManager/DaemonJobTxnGC.h>
#include <gtest/gtest.h>


using namespace DB::DaemonManager;
using namespace DB;

namespace GtestDaemonTxnGC
{

TEST(GlobalTxnGCTest, extractLastElements)
{
    {
        std::vector<TxnTimestamp> from{1, 2, 3, 4, 5, 6};

        {
            std::vector<TxnTimestamp> extract = extractLastElements(from, 0);
            EXPECT_TRUE(extract.empty());
            EXPECT_EQ(from.size(), 6);
        }

        {
            std::vector<TxnTimestamp> extract = extractLastElements(from, 2);
            std::vector<TxnTimestamp> expected_extract {5, 6};
            std::vector<TxnTimestamp> expected_from {1, 2, 3, 4};
            EXPECT_EQ(from, expected_from);
            EXPECT_EQ(extract, expected_extract);
        }

        {
            std::vector<TxnTimestamp> extract = extractLastElements(from, 5);
            std::vector<TxnTimestamp> expected_extract {1,2, 3, 4};
            EXPECT_EQ(extract, expected_extract);
            EXPECT_TRUE(from.empty());
        }
    }

    {
        std::vector<TxnTimestamp> from{1, 2, 3, 4, 5, 6, 7};
        std::vector<TxnTimestamp> extract = extractLastElements(from, 7);
        std::vector<TxnTimestamp> expected_extract {1, 2, 3, 4, 5, 6, 7};
        EXPECT_EQ(extract, expected_extract);
        EXPECT_TRUE(from.empty());
    }
}

}
