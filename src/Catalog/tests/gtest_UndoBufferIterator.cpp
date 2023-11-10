#include <Catalog/Catalog.h>
#include <gtest/gtest.h>

using namespace DB;
namespace
{


TEST(UndoBufferIterator, InvalidTest)
{
    Catalog::Catalog::UndoBufferIterator it(nullptr, nullptr);
    EXPECT_FALSE(it.next());
    EXPECT_FALSE(it.is_valid());
    EXPECT_THROW(it.getUndoResource(), Exception);
}

}
