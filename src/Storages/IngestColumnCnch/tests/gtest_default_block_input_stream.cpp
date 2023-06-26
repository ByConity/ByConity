#include <Storages/IngestColumnCnch/DefaultBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <string>
#include <gtest/gtest.h>

using namespace DB;
namespace
{

Block getHeader()
{
    Block res;

    {
        ColumnWithTypeAndName column1;
        column1.name = "i";
        column1.type = std::make_shared<DataTypeInt32>();
        res.insert(column1);
    }

    {
        ColumnWithTypeAndName column2;
        column2.name = "s";
        column2.type = std::make_shared<DataTypeString>();
        res.insert(column2);
    }

    {
        ColumnWithTypeAndName column3;
        column3.name = "d";
        column3.type = std::make_shared<DataTypeString>();
        res.insert(column3);
    }

    return res;
}

TEST(default_block_input_stream, empty_rows)
{
    DefaultBlockInputStream s{getHeader(), 0, 10};

    s.readPrefix();
    Block b = s.read();
    bool empty = (b == Block{});
    EXPECT_TRUE(empty);

    s.readSuffix();
}


TEST(default_block_input_stream, one_block)
{
    DefaultBlockInputStream s{getHeader(), 1, 10};
    s.readPrefix();
    Block b = s.read();
    EXPECT_EQ(b.rows(), 1);
    EXPECT_EQ(b.columns(), 3);
    ColumnPtr col_i = b.getByName("i").column;
    EXPECT_EQ((*col_i)[0].get<Int64>(), 0);

    ColumnPtr col_s = b.getByName("s").column;
    EXPECT_EQ((*col_s)[0].get<String>(), "");

    ColumnPtr col_d = b.getByName("d").column;
    EXPECT_EQ((*col_d)[0].get<String>(), "");

    Block end_block = s.read();
    bool empty = (end_block == Block{});
    EXPECT_TRUE(empty);
    s.readSuffix();
}

TEST(default_block_input_stream, two_block)
{
    DefaultBlockInputStream s{getHeader(), 3, 2};
    s.readPrefix();

    {
        Block block1 = s.read();
        EXPECT_EQ(block1.rows(), 2);
        EXPECT_EQ(block1.columns(), 3);
        ColumnPtr col_i = block1.getByName("i").column;
        EXPECT_EQ((*col_i)[0].get<Int64>(), 0);

        ColumnPtr col_s = block1.getByName("s").column;
        EXPECT_EQ((*col_s)[0].get<String>(), "");

        ColumnPtr col_d = block1.getByName("d").column;
        EXPECT_EQ((*col_d)[0].get<String>(), "");
    }

    {
        Block block2 = s.read();
        EXPECT_EQ(block2.rows(), 1);
        EXPECT_EQ(block2.columns(), 3);
        ColumnPtr col_i = block2.getByName("i").column;
        EXPECT_EQ((*col_i)[0].get<Int64>(), 0);

        ColumnPtr col_s = block2.getByName("s").column;
        EXPECT_EQ((*col_s)[0].get<String>(), "");

        ColumnPtr col_d = block2.getByName("d").column;
        EXPECT_EQ((*col_d)[0].get<String>(), "");
    }

    {
        Block end_block = s.read();
        bool empty = (end_block == Block{});
        EXPECT_TRUE(empty);
    }
    s.readSuffix();
}

} /// end anonymous namespace
