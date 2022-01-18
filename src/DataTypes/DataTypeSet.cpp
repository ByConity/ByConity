#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{

static DataTypePtr create()
{
    return std::make_shared<DataTypeSet>();
}

void registerDataTypeSet(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("Set", create);
}

}
