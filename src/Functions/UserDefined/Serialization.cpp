#include <sys/types.h>
#include <functional>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include "Proto.h"
#include "Serialization.h"
#include "VectorType.h"

namespace DB {

namespace ErrorCodes {
    extern const int UNSUPPORTED_METHOD;
}


/* create shared memory and push the index, store mapping of output column */
void pushStrShm(struct metastore *ms,
                PaddedPODArray<UInt8> &arr,
                uint64_t prefix,
                uint16_t *idx) {
    ms->output.emplace_back(arr.createOutStrShm(prefix, *idx));
    (*idx)++;
}

template<typename T>
void pushShm(struct metastore *ms,
             PaddedPODArray<T> &arr,
             uint64_t prefix,
             uint16_t *idx)
{
    if (ms->isOut) {
        /* output column */
        ms->output.emplace_back(*arr.createShm(prefix, *idx, true));
    } else {
        arr.createShm(prefix, *idx);
    }

    (*idx)++;
}

template <typename Type>
Type * constGetColumn(const IColumn *column)
{
    return typeid_cast<Type *>(const_cast<IColumn *>(column));
}

template<typename ColType>
void serializeDecimal(const IColumn *c,
                      struct metastore *ms,
                      struct UDFMeta &m,
                      uint64_t prefix,
                      uint16_t *idx)
{
    auto *i = constGetColumn<ColType>(c);

    pushShm(ms, i->getData(), prefix, idx);
    m.pushExtra(ms->extras, i->getScale());
}

template<typename ColType>
void serializeVector(const IColumn *c,
                     struct metastore *ms,
                     uint64_t prefix,
                     uint16_t *idx)
{
    auto *i = constGetColumn<ColumnVector<ColType>>(c);

    pushShm(ms, i->getData(), prefix, idx);
}

void serializeColumn(const ColumnWithTypeAndName &col,
                     struct metastoreex *mse)
{
    struct UDFMeta m = {};
    const IColumn *c;
    TypeIndex id;
    ColumnPtr cp;

    if (isColumnConst(*col.column.get())) {
        cp = col.column->convertToFullColumnIfConst();
        mse->ptrs.push_back(cp);
    } else {
        cp = col.column;
    }

    id = col.type->getTypeId();
    m.type = static_cast<uint8_t>(id);
    m.fd_cnt = 1;

    // TODO: implement LowCardinality later
    if (id == TypeIndex::LowCardinality) {
        cp = cp->convertToFullColumnIfLowCardinality();
        mse->ptrs.push_back(cp);
    }

    c = cp.get();
    switch (id) {
#define V(IDX, TYPE) \
        case TypeIndex::IDX: \
            serializeVector<TYPE>(c, &mse->ms, mse->prefix, &mse->idx); \
        break;
        APPLY_FOR_EACH_VECTOR_TYPE(V)
#undef V

        case TypeIndex::Decimal32:
            serializeDecimal<ColumnDecimal<Decimal32>>(c, &mse->ms, m, mse->prefix, &mse->idx);
            break;
        case TypeIndex::Decimal64:
            serializeDecimal<ColumnDecimal<Decimal64>>(c, &mse->ms, m, mse->prefix, &mse->idx);
            break;
        case TypeIndex::Decimal128:
            serializeDecimal<ColumnDecimal<Decimal128>>(c, &mse->ms, m, mse->prefix, &mse->idx);
            break;
        case TypeIndex::String: {
            auto *i = constGetColumn<ColumnString>(c);

            pushShm(&mse->ms, i->getOffsets(), mse->prefix, &mse->idx);
            if (mse->ms.isOut)
                pushStrShm(&mse->ms, i->getChars(), mse->prefix, &mse->idx);
            else
                pushShm(&mse->ms, i->getChars(), mse->prefix, &mse->idx);
            m.fd_cnt = 2;
            break;
        }
        case TypeIndex::FixedString: {
            auto *i = constGetColumn<ColumnFixedString>(c);

            pushShm(&mse->ms, i->getChars(), mse->prefix, &mse->idx);
            m.pushExtra(mse->ms.extras, i->getN());
            break;
        }
        case TypeIndex::Nullable: {
            auto *i = constGetColumn<ColumnNullable>(c);

            pushShm(&mse->ms, i->getNullMapColumn().getData(), mse->prefix, &mse->idx);
            m.flags = BIT(UDFMetaFlags::HAS_NESTED_DATA);
            break;
        }
        case TypeIndex::Array: {
            auto *i = constGetColumn<ColumnArray>(c);

            pushShm(&mse->ms, i->getOffsets(), mse->prefix, &mse->idx);
            m.flags = BIT(UDFMetaFlags::HAS_NESTED_DATA);
            break;
        }
        case TypeIndex::Tuple: {
            auto *i = constGetColumn<ColumnTuple>(c);
            const auto &tuple_columns = i->getColumns();
            const IDataType *type = col.type.get();

            mse->ms.extras.push_back(tuple_columns.size());
            mse->ms.metas.push_back(m);

            const auto &elements =
                checkAndGetDataType<DataTypeTuple>(type)->getElements();

            for(size_t idx = 0; idx < tuple_columns.size(); idx++)
            {
                serializeColumn(
                    ColumnWithTypeAndName(tuple_columns[idx]->getPtr(),
                                          elements[idx],
                                          tuple_columns[idx]->getName()),
                    mse);
            }
            return;
        }
        case TypeIndex::Set:
        case TypeIndex::Interval:
        case TypeIndex::Function:
        case TypeIndex::AggregateFunction:
        case TypeIndex::Map:
        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
        case TypeIndex::LowCardinality:
        default:
            throw Exception("non implemented UDF type",
                            ErrorCodes::UNSUPPORTED_METHOD);
    }

    mse->ms.metas.push_back(m);

    if (m.flags & BIT(UDFMetaFlags::HAS_NESTED_DATA))
        serializeColumn(columnGetNested(col), mse);
}
}
