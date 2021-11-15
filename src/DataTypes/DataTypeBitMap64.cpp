
#include <DataTypes/DataTypeBitMap64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationBitMap64.h>
#include <Columns/ColumnBitMap64.h>

#ifdef __SSE2__
#include <emmintrin.h>
#endif


namespace DB
{

MutableColumnPtr DataTypeBitMap64::createColumn() const
{
    return ColumnBitMap64::create();
}

bool DataTypeBitMap64::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeBitMap64::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationBitMap64>();
}

static DataTypePtr create()
{
    return std::make_shared<DataTypeBitMap64>();
}

void registerDataTypeBitMap64(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("BitMap64", create);
}

}
