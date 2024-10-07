#include <DataTypes/DataTypeJsonb.h>
#include <Columns/ColumnJsonb.h>
#include <DataTypes/Serializations/SerializationJsonb.h>
#include <DataTypes/Serializations/JSONBValue.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

Field DataTypeJsonb::getDefault() const
{
    std::string default_json = "{}";
    JsonBinaryValue binary_val(default_json.c_str(), default_json.size());
    return JsonbField(binary_val.value(), binary_val.size());
}

MutableColumnPtr DataTypeJsonb::createColumn() const
{
    return ColumnJsonb::create();
}

bool DataTypeJsonb::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeJsonb::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationJsonb>();
}

void registerDataTypeJSONB(DataTypeFactory & factory)
{
    auto creator = static_cast<DataTypePtr(*)()>([] { return DataTypePtr(std::make_shared<DataTypeJsonb>()); });
    factory.registerSimpleDataType("JSONB", creator, DataTypeFactory::CaseInsensitive);
}

} // namespace DB
