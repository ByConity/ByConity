#include <DataTypes/DataTypeFactory.h>

#include <DataTypes/DataTypeTime.h>
#include <DataTypes/Serializations/SerializationTime.h>

#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <string>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

static constexpr UInt32 max_scale = 9;

DataTypeTime::DataTypeTime(UInt32 scale_)
    : DataTypeDecimalBase<Decimal64>(DecimalUtils::max_precision<Decimal64>, scale_)
{
    if (scale > max_scale)
        throw Exception("Scale " + std::to_string(scale) + " is too large for Time. Maximum is up to nanoseconds (9).",
            ErrorCodes::ARGUMENT_OUT_OF_BOUND);
}

std::string DataTypeTime::doGetName() const
{
    WriteBufferFromOwnString out;
    out << "Time(" << this->scale << ")";
    return out.str();
}

bool DataTypeTime::equals(const IDataType & rhs) const
{
    if (const auto * ptype = typeid_cast<const DataTypeTime *>(&rhs))
        return this->scale == ptype->getScale();
    return false;
}

SerializationPtr DataTypeTime::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationTime>(scale);
}

}
