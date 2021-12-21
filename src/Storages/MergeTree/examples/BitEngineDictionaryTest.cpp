#include <iostream>

#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionary.h>


namespace DB
{

int main(int, char **)
{
    String path = "/data01/liuhaoqiang/biengine_dict_test";
    String name = "id_map";
    BitEngineDictionary dict(path, name, 0ULL);
    BitMap64 bitmap = roaring::Roaring64Map::bitmapOf(1,2,3,4,5);
    PODArray<UInt64> res_arr;
    auto res = dict.encodeImpl(bitmap, res_arr);
    std::cout << "res: " << res.toString() << std::endl;
    return 0;
}

}
