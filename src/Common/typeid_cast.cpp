#include "common/types.h"
#include <Common/typeid_cast.h>

#include <Columns/ColumnVector.h>

template <typename Vtype, typename From>
bool typeid_cast_colvec(From & from, bool try_flush) {
    if ((typeid(from) == typeid(DB::ColumnVector<Vtype, true>)) || (typeid(from) == typeid(DB::ColumnVector<Vtype, false>)) ) {
        if (!try_flush || (reinterpret_cast<const DB::ColumnVector<Vtype> &>(from).getZeroCopyBuf().size() == 0)) {
            return true;
        }
        // if (try_flush && (reinterpret_cast<const DB::ColumnVector<Vtype> &>(from).getZeroCopyBuf().size())) {
        from.tryToFlushZeroCopyBuffer();
        // }
        return true;
    } else {
        return false;
    }
}

template bool typeid_cast_colvec<DB::UInt16, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::UInt16, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<DB::UInt32, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::UInt32, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<DB::UInt64, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::UInt64, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<DB::UInt128, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::UInt128, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<DB::UInt256, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::UInt256, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<DB::Int16, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::Int16, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<DB::Int32, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::Int32, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<DB::Int64, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::Int64, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<DB::Int128, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::Int128, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<DB::Int256, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::Int256, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<DB::Float32, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::Float32, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<DB::Float64, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::Float64, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<DB::UUID, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<DB::UUID, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<signed char, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<signed char, DB::IColumn const>(DB::IColumn const&, bool);
template bool typeid_cast_colvec<char8_t, DB::IColumn>(DB::IColumn&, bool);
template bool typeid_cast_colvec<char8_t, DB::IColumn const>(DB::IColumn const&, bool);

template bool typeid_cast_colvec<char8_t, DB::ColumnVector<char8_t, false> >(DB::ColumnVector<char8_t, false>&, bool);
template bool typeid_cast_colvec<char8_t, DB::ColumnVector<char8_t, false> const>(DB::ColumnVector<char8_t, false> const&, bool);
template bool typeid_cast_colvec<DB::UInt16, DB::ColumnVector<DB::UInt16, false> >(DB::ColumnVector<DB::UInt16, false>&, bool);
template bool typeid_cast_colvec<DB::UInt32, DB::ColumnVector<DB::UInt32, false> >(DB::ColumnVector<DB::UInt32, false>&, bool);
template bool typeid_cast_colvec<DB::UInt64, DB::ColumnVector<DB::UInt64, false> >(DB::ColumnVector<DB::UInt64, false>&, bool);
template bool typeid_cast_colvec<DB::UInt128, DB::ColumnVector<DB::UInt128, false> >(DB::ColumnVector<DB::UInt128, false>&, bool);
template bool typeid_cast_colvec<DB::UInt256, DB::ColumnVector<DB::UInt256, false> >(DB::ColumnVector<DB::UInt256, false>&, bool);
template bool typeid_cast_colvec<DB::Int16, DB::ColumnVector<DB::Int16, false> >(DB::ColumnVector<DB::Int16, false>&, bool);
template bool typeid_cast_colvec<DB::Int32, DB::ColumnVector<DB::Int32, false> >(DB::ColumnVector<DB::Int32, false>&, bool);
template bool typeid_cast_colvec<DB::Int64, DB::ColumnVector<DB::Int64, false> >(DB::ColumnVector<DB::Int64, false>&, bool);
template bool typeid_cast_colvec<DB::Int128, DB::ColumnVector<DB::Int128, false> >(DB::ColumnVector<DB::Int128, false>&, bool);
template bool typeid_cast_colvec<DB::Int256, DB::ColumnVector<DB::Int256, false> >(DB::ColumnVector<DB::Int256, false>&, bool);
template bool typeid_cast_colvec<DB::Float32, DB::ColumnVector<DB::Float32, false> >(DB::ColumnVector<DB::Float32, false>&, bool);
template bool typeid_cast_colvec<DB::Float64, DB::ColumnVector<DB::Float64, false> >(DB::ColumnVector<DB::Float64, false>&, bool);
template bool typeid_cast_colvec<DB::UUID, DB::ColumnVector<DB::UUID, false> >(DB::ColumnVector<DB::UUID, false>&, bool);



template bool typeid_cast_colvec<DB::UInt16, DB::ColumnVector<DB::UInt16, false> const>(DB::ColumnVector<DB::UInt16, false> const&, bool);
template bool typeid_cast_colvec<DB::UInt32, DB::ColumnVector<DB::UInt32, false> const>(DB::ColumnVector<DB::UInt32, false> const&, bool);
template bool typeid_cast_colvec<DB::UInt64, DB::ColumnVector<DB::UInt64, false> const>(DB::ColumnVector<DB::UInt64, false> const&, bool);
template bool typeid_cast_colvec<DB::UInt128, DB::ColumnVector<DB::UInt128, false> const>(DB::ColumnVector<DB::UInt128, false> const&, bool);
template bool typeid_cast_colvec<DB::UInt256, DB::ColumnVector<DB::UInt256, false> const>(DB::ColumnVector<DB::UInt256, false> const&, bool);
template bool typeid_cast_colvec<DB::Int16, DB::ColumnVector<DB::Int16, false> const>(DB::ColumnVector<DB::Int16, false> const&, bool);
template bool typeid_cast_colvec<DB::Int32, DB::ColumnVector<DB::Int32, false> const>(DB::ColumnVector<DB::Int32, false> const&, bool);
template bool typeid_cast_colvec<DB::Int64, DB::ColumnVector<DB::Int64, false> const>(DB::ColumnVector<DB::Int64, false> const&, bool);
template bool typeid_cast_colvec<DB::Int128, DB::ColumnVector<DB::Int128, false> const>(DB::ColumnVector<DB::Int128, false> const&, bool);
template bool typeid_cast_colvec<DB::Int256, DB::ColumnVector<DB::Int256, false> const>(DB::ColumnVector<DB::Int256, false> const&, bool);
template bool typeid_cast_colvec<DB::Float32, DB::ColumnVector<DB::Float32, false> const>(DB::ColumnVector<DB::Float32, false> const&, bool);
template bool typeid_cast_colvec<DB::Float64, DB::ColumnVector<DB::Float64, false> const>(DB::ColumnVector<DB::Float64, false> const&, bool);
template bool typeid_cast_colvec<DB::UUID, DB::ColumnVector<DB::UUID, false> const>(DB::ColumnVector<DB::UUID, false> const&, bool);
