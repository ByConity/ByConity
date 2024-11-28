#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <common/types.h>
#include <Core/Types.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Core/Block.h>
#include <Storages/MergeTree/MergeTreeIndexInvertedHelper.h>

using namespace DB;

class Node
{
public:
    String name;
    std::vector<Node> child_indexes;
    explicit Node(const String & name_) : name(name_){}
};

void build_indexed_block_1(Block & block)
{
   // build sub column
    DataTypes data_types_in_doc;
    Strings data_types_name_in_doc;
    // key1
    ColumnPtr key1_column =  ColumnString::create();
    DataTypePtr key1_data_type = std::make_shared<DataTypeString>();
    
    data_types_in_doc.push_back(key1_data_type);
    data_types_name_in_doc.push_back("key1");
    // key2
    ColumnPtr key21_column =  ColumnString::create();
    DataTypePtr key21_data_type = std::make_shared<DataTypeString>();
    DataTypes data_types_in_key2 ;
    Strings data_types_name_in_key2;
    data_types_in_key2.push_back(key21_data_type);
    data_types_name_in_key2.push_back("key21");
    
    ColumnPtr key2_column =  ColumnTuple::create(Columns{key21_column});
    DataTypePtr key2_data_type = std::make_shared<DataTypeTuple>(data_types_in_key2, data_types_name_in_key2);
    data_types_in_doc.push_back(key2_data_type);
    data_types_name_in_doc.push_back("key2");
    // key4
    ColumnPtr key41_column =  ColumnString::create();
    DataTypePtr key41_data_type = std::make_shared<DataTypeString>();
    ColumnPtr key42_column =  ColumnString::create();
    DataTypePtr key42_data_type = std::make_shared<DataTypeString>();
    DataTypes data_types_in_key4 ;
    Strings data_types_name_in_key4;
    data_types_in_key4.push_back(key41_data_type);
    data_types_name_in_key4.push_back("key41");
    data_types_in_key4.push_back(key42_data_type);
    data_types_name_in_key4.push_back("key42");
    
    ColumnPtr key4_column =  ColumnTuple::create(Columns{key41_column, key42_column});
    DataTypePtr key4_data_type = std::make_shared<DataTypeTuple>(data_types_in_key4, data_types_name_in_key4);
    data_types_in_doc.push_back(key4_data_type);
    data_types_name_in_doc.push_back("key4");
    // build column
    
    DataTypePtr doc_data_type = std::make_shared<DataTypeTuple>(data_types_in_doc, data_types_name_in_doc);
    ColumnPtr doc_column = ColumnTuple::create(Columns{key1_column, key2_column, key4_column});
    
    // insert column in block
    ColumnWithTypeAndName doc_column_with_type;
    doc_column_with_type.name = "doc";
    doc_column_with_type.type = doc_data_type;
    doc_column_with_type.column = doc_column;
    block.insert(doc_column_with_type);
}
void buildTreeFromNodeImpl(std::stringstream & sstream, const std::string & prefix, const Node & node)
{
    sstream << prefix << node.name << "\n";
    for (const auto & index : node.child_indexes)
    {
        buildTreeFromNodeImpl(sstream, prefix + "|--", index);
    }
}
// test tree
// doc
// |--key1(indexed)
// |--key2
// |--|--key21(indexed)
// |--key3
// |--|--key31(indexed)
// |--key4(indexed)
String buildTreeFromNodeCase1()
{
    std::stringstream sstream;
    Node node_doc("doc");
    
    Node node_doc_key1("key1(indexed)");
    
    Node node_doc_key2("key2");
    Node node_doc_key2_key21("key21(indexed)");
    
    Node node_doc_key3("key3");
    Node node_doc_key3_key31("key31(indexed)");
    Node node_doc_key4("key4(indexed)");
    node_doc_key2.child_indexes.emplace_back(node_doc_key2_key21);
    node_doc_key3.child_indexes.emplace_back(node_doc_key3_key31);
    node_doc.child_indexes.emplace_back(node_doc_key1);
    node_doc.child_indexes.emplace_back(node_doc_key2);
    node_doc.child_indexes.emplace_back(node_doc_key3);
    node_doc.child_indexes.emplace_back(node_doc_key4);
    buildTreeFromNodeImpl(sstream, "", node_doc);
    return sstream.str();
}

// test basic build
TEST(JSONPathBuild, BuildPath1)
{
    // build test block
    Block block;
    String column_name("doc");
    std::vector<String> paths; 
    paths.push_back("key1");
    paths.push_back("key2.key21");
    paths.push_back("key3.key31");
    paths.push_back("key4");
    // build index path
    IndexColumnPath index_builder(column_name, paths);
    EXPECT_EQ(index_builder.printNodeTree(),buildTreeFromNodeCase1());
}

String buildTreeFromNodeCase2()
{
    return "doc(indexed)\n";
}

// test build with ["*"]
TEST(JSONPathBuild, BuildPath2)
{
    // build test block
    Block block;
    String column_name("doc");
    std::vector<String> paths; 
    paths.push_back("*");
    // build index path
    IndexColumnPath index_builder(column_name, paths);
    EXPECT_EQ(index_builder.printNodeTree(), buildTreeFromNodeCase2());
}
