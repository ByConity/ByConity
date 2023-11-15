#include <Common/RowExistsColumnInfo.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{
    const NameAndTypePair RowExistsColumn::ROW_EXISTS_COLUMN{"_row_exists", std::make_shared<DataTypeUInt8>()};

}
