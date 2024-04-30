#include <Catalog/CatalogUtils.h>
#include <gtest/gtest.h>

namespace
{

using namespace DB;

TEST(CatalogUtils, parseTxnIdFromUndoBufferKey)
{
    UInt64 txn_id;
    ASSERT_TRUE(Catalog::parseTxnIdFromUndoBufferKey("default_UB_449165848378081952_[fdbd:dc03:1:441::12]:9457_392948", txn_id));
    ASSERT_EQ(txn_id, 449165848378081952ul);

    ASSERT_TRUE(Catalog::parseTxnIdFromUndoBufferKey("default_UB_037903929070839844R_[fdbd:dc03:12:104::205]:9373_884845", txn_id));
    ASSERT_EQ(txn_id, 448938070929309730ul);

    ASSERT_FALSE(Catalog::parseTxnIdFromUndoBufferKey("default_UB_449165848378081952", txn_id));
    ASSERT_FALSE(Catalog::parseTxnIdFromUndoBufferKey("default_UB_037903929070839844R", txn_id));
}

}
