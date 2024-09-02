#pragma once

#include <type_traits>
#include <typeinfo>
#include <typeindex>
#include <memory>
#include <string>

#include "common/types.h"
#include <common/shared_ptr_helper.h>
#include <Common/Exception.h>
#include <common/demangle.h>
#include <Core/Types.h>
#include <iostream>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
    }

    template <typename T, bool has_buf>
    class ColumnVector;

    class IColumn;
}


template<typename T, typename U>
constexpr bool is_decay_equ = std::is_same_v<std::decay_t<T>, std::decay_t<U>>;

template<typename B, typename D>
constexpr bool is_decay_baseof = std::is_base_of_v<std::decay_t<B>, std::decay_t<D>>;

template <typename To, typename VType>
constexpr bool is_vtype_vec = is_decay_equ<To, DB::ColumnVector<VType, false>> || 
            is_decay_equ<To, DB::ColumnVector<VType, true>>;

template <typename To, typename From, typename VType>
constexpr bool is_same_vtype_vec = (is_vtype_vec<To, VType>) && (is_vtype_vec<From, VType>);

template <typename Vtype, typename From>
bool typeid_cast_colvec(From & from, bool try_flush = true);

extern template bool typeid_cast_colvec<DB::UInt16, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::UInt16, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<DB::UInt32, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::UInt32, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<DB::UInt64, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::UInt64, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<DB::UInt128, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::UInt128, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<DB::UInt256, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::UInt256, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<DB::Int16, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::Int16, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<DB::Int32, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::Int32, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<DB::Int64, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::Int64, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<DB::Int128, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::Int128, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<DB::Int256, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::Int256, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<DB::Float32, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::Float32, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<DB::Float64, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::Float64, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<DB::UUID, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<DB::UUID, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<signed char, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<signed char, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<char8_t, DB::IColumn>(DB::IColumn&, bool);
extern template bool typeid_cast_colvec<char8_t, DB::IColumn const>(DB::IColumn const&, bool);
extern template bool typeid_cast_colvec<char8_t, DB::ColumnVector<char8_t, false> >(DB::ColumnVector<char8_t, false>&, bool);
extern template bool typeid_cast_colvec<char8_t, DB::ColumnVector<char8_t, false> const>(DB::ColumnVector<char8_t, false> const&, bool);

extern template bool typeid_cast_colvec<DB::UInt16, DB::ColumnVector<DB::UInt16, false> >(DB::ColumnVector<DB::UInt16, false>&, bool);
extern template bool typeid_cast_colvec<DB::UInt32, DB::ColumnVector<DB::UInt32, false> >(DB::ColumnVector<DB::UInt32, false>&, bool);
extern template bool typeid_cast_colvec<DB::UInt64, DB::ColumnVector<DB::UInt64, false> >(DB::ColumnVector<DB::UInt64, false>&, bool);
extern template bool typeid_cast_colvec<DB::UInt128, DB::ColumnVector<DB::UInt128, false> >(DB::ColumnVector<DB::UInt128, false>&, bool);
extern template bool typeid_cast_colvec<DB::UInt256, DB::ColumnVector<DB::UInt256, false> >(DB::ColumnVector<DB::UInt256, false>&, bool);
extern template bool typeid_cast_colvec<DB::Int16, DB::ColumnVector<DB::Int16, false> >(DB::ColumnVector<DB::Int16, false>&, bool);
extern template bool typeid_cast_colvec<DB::Int32, DB::ColumnVector<DB::Int32, false> >(DB::ColumnVector<DB::Int32, false>&, bool);
extern template bool typeid_cast_colvec<DB::Int64, DB::ColumnVector<DB::Int64, false> >(DB::ColumnVector<DB::Int64, false>&, bool);
extern template bool typeid_cast_colvec<DB::Int128, DB::ColumnVector<DB::Int128, false> >(DB::ColumnVector<DB::Int128, false>&, bool);
extern template bool typeid_cast_colvec<DB::Int256, DB::ColumnVector<DB::Int256, false> >(DB::ColumnVector<DB::Int256, false>&, bool);
extern template bool typeid_cast_colvec<DB::Float32, DB::ColumnVector<DB::Float32, false> >(DB::ColumnVector<DB::Float32, false>&, bool);
extern template bool typeid_cast_colvec<DB::Float64, DB::ColumnVector<DB::Float64, false> >(DB::ColumnVector<DB::Float64, false>&, bool);
extern template bool typeid_cast_colvec<DB::UUID, DB::ColumnVector<DB::UUID, false> >(DB::ColumnVector<DB::UUID, false>&, bool);



extern template bool typeid_cast_colvec<DB::UInt16, DB::ColumnVector<DB::UInt16, false> const>(DB::ColumnVector<DB::UInt16, false> const&, bool);
extern template bool typeid_cast_colvec<DB::UInt32, DB::ColumnVector<DB::UInt32, false> const>(DB::ColumnVector<DB::UInt32, false> const&, bool);
extern template bool typeid_cast_colvec<DB::UInt64, DB::ColumnVector<DB::UInt64, false> const>(DB::ColumnVector<DB::UInt64, false> const&, bool);
extern template bool typeid_cast_colvec<DB::UInt128, DB::ColumnVector<DB::UInt128, false> const>(DB::ColumnVector<DB::UInt128, false> const&, bool);
extern template bool typeid_cast_colvec<DB::UInt256, DB::ColumnVector<DB::UInt256, false> const>(DB::ColumnVector<DB::UInt256, false> const&, bool);
extern template bool typeid_cast_colvec<DB::Int16, DB::ColumnVector<DB::Int16, false> const>(DB::ColumnVector<DB::Int16, false> const&, bool);
extern template bool typeid_cast_colvec<DB::Int32, DB::ColumnVector<DB::Int32, false> const>(DB::ColumnVector<DB::Int32, false> const&, bool);
extern template bool typeid_cast_colvec<DB::Int64, DB::ColumnVector<DB::Int64, false> const>(DB::ColumnVector<DB::Int64, false> const&, bool);
extern template bool typeid_cast_colvec<DB::Int128, DB::ColumnVector<DB::Int128, false> const>(DB::ColumnVector<DB::Int128, false> const&, bool);
extern template bool typeid_cast_colvec<DB::Int256, DB::ColumnVector<DB::Int256, false> const>(DB::ColumnVector<DB::Int256, false> const&, bool);
extern template bool typeid_cast_colvec<DB::Float32, DB::ColumnVector<DB::Float32, false> const>(DB::ColumnVector<DB::Float32, false> const&, bool);
extern template bool typeid_cast_colvec<DB::Float64, DB::ColumnVector<DB::Float64, false> const>(DB::ColumnVector<DB::Float64, false> const&, bool);
extern template bool typeid_cast_colvec<DB::UUID, DB::ColumnVector<DB::UUID, false> const>(DB::ColumnVector<DB::UUID, false> const&, bool);


/** Checks type by comparing typeid.
  * The exact match of the type is checked. That is, cast to the ancestor will be unsuccessful.
  * In the rest, behaves like a dynamic_cast.
  */
template <typename To, typename From>
std::enable_if_t<std::is_reference_v<To>, To> typeid_cast(From & from, bool try_flush_zerocpy_buf = true)
{
    try
    {

        if constexpr (is_decay_baseof<From, To>) { 
            if constexpr (is_vtype_vec<To, DB::UInt8> ) {
                return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::UInt16> ) {
                if (typeid_cast_colvec<DB::UInt16>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::UInt32> ) {
                if (typeid_cast_colvec<DB::UInt32>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::UInt64> ) {
                if (typeid_cast_colvec<DB::UInt64>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::UInt128> ) {
                if (typeid_cast_colvec<DB::UInt128>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::UInt256> ) {
                if (typeid_cast_colvec<DB::UInt256>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr (is_vtype_vec<To, DB::Int8> ) {
                if (typeid_cast_colvec<DB::Int8>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::Int16> ) {
                if (typeid_cast_colvec<DB::Int16>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::Int32> ) {
                if (typeid_cast_colvec<DB::Int32>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::Int64> ) {
                if (typeid_cast_colvec<DB::Int64>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::Int128> ) {
                if (typeid_cast_colvec<DB::Int128>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::Int256> ) {
                if (typeid_cast_colvec<DB::Int256>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::Float32> ) {
                if (typeid_cast_colvec<DB::Float32>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::Float64> ) {
                if (typeid_cast_colvec<DB::Float64>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<To, DB::UUID> ) {
                if (typeid_cast_colvec<DB::UUID>(from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            }
        }  else if constexpr(is_same_vtype_vec<To, From, DB::UInt8>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::UInt16>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::UInt32>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::UInt64>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::UInt128>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::UInt256>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::Int8>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::Int16>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::Int32>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::Int64>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::Int128>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::Int256>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::Float32>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::Float64>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<To, From, DB::UUID>) {
            return static_cast<To>(from);
        }  

        if ((typeid(From) == typeid(To)) || (typeid(from) == typeid(To)))
            return static_cast<To>(from);
    }
    catch (const std::exception & e)
    {
        throw DB::Exception(e.what(), DB::ErrorCodes::LOGICAL_ERROR);
    }
    throw DB::Exception("Bad cast from type " + demangle(typeid(from).name()) + " to " + demangle(typeid(To).name()),
                        DB::ErrorCodes::LOGICAL_ERROR);
}


template <typename To, typename From>
std::enable_if_t<std::is_pointer_v<To>, To> typeid_cast(From * from, bool try_flush_zerocpy_buf = true)
{
    try
    {

         if constexpr (is_decay_baseof<From,  std::remove_pointer_t<To>>) { 
            if constexpr (is_vtype_vec<std::remove_pointer_t<To>, DB::UInt8> ) {
                if (from && typeid_cast_colvec<DB::UInt8>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::UInt16> ) {
                if (from && typeid_cast_colvec<DB::UInt16>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::UInt32> ) {
                if (from && typeid_cast_colvec<DB::UInt32>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::UInt64> ) {
                if (from && typeid_cast_colvec<DB::UInt64>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::UInt128> ) {
                if (from && typeid_cast_colvec<DB::UInt128>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::UInt256> ) {
                if (from && typeid_cast_colvec<DB::UInt256>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr (is_vtype_vec<std::remove_pointer_t<To>, DB::Int8> ) {
                if (from && typeid_cast_colvec<DB::Int8>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::Int16> ) {
                if (from && typeid_cast_colvec<DB::Int16>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::Int32> ) {
                if (from && typeid_cast_colvec<DB::Int32>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::Int64> ) {
                if (from && typeid_cast_colvec<DB::Int64>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::Int128> ) {
                if (from && typeid_cast_colvec<DB::Int128>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::Int256> ) {
                if (from && typeid_cast_colvec<DB::Int256>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::Float32> ) {
                if (from && typeid_cast_colvec<DB::Float32>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::Float64> ) {
                if (from && typeid_cast_colvec<DB::Float64>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<To>, DB::UUID> ) {
                if (from && typeid_cast_colvec<DB::UUID>(*from, try_flush_zerocpy_buf)) 
                    return static_cast<To>(from);
            }
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::UInt8>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::UInt16>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::UInt32>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::UInt64>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::UInt128>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::UInt256>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::Int8>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::Int16>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::Int32>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::Int64>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::Int128>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::Int256>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::Float32>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::Float64>) {
            return static_cast<To>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<To>, From, DB::UUID>) {
            return static_cast<To>(from);
        }  

        if ((typeid(From) == typeid(std::remove_pointer_t<To>)) || (from && typeid(*from) == typeid(std::remove_pointer_t<To>)))
            return static_cast<To>(from);
        else
            return nullptr;
    }
    catch (const std::exception & e)
    {
        throw DB::Exception(e.what(), DB::ErrorCodes::LOGICAL_ERROR);
    }
}


template <typename To, typename From>
std::enable_if_t<is_shared_ptr_v<To>, To> typeid_cast(const std::shared_ptr<From> & from, bool try_flush_zerocpy_buf = true)
{
    try
    {
        if constexpr (is_decay_baseof<From,  std::remove_pointer_t<typename To::element_type>>) {
            if constexpr (is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::UInt8> ) {
                if (from && typeid_cast_colvec<DB::UInt8>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::UInt16> ) {
                if (from && typeid_cast_colvec<DB::UInt16>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::UInt32> ) {
                if (from && typeid_cast_colvec<DB::UInt32>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::UInt64> ) {
                if (from && typeid_cast_colvec<DB::UInt64>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::UInt128> ) {
                if (from && typeid_cast_colvec<DB::UInt128>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::UInt256> ) {
                if (from && typeid_cast_colvec<DB::UInt256>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr (is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::Int8> ) {
                if (from && typeid_cast_colvec<DB::Int8>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::Int16> ) {
                if (from && typeid_cast_colvec<DB::Int16>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::Int32> ) {
                if (from && typeid_cast_colvec<DB::Int32>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::Int64> ) {
                if (from && typeid_cast_colvec<DB::Int64>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::Int128> ) {
                if (from && typeid_cast_colvec<DB::Int128>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::Int256> ) {
                if (from && typeid_cast_colvec<DB::Int256>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::Float32> ) {
                if (from && typeid_cast_colvec<DB::Float32>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::Float64> ) {
                if (from && typeid_cast_colvec<DB::Float64>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            } else if constexpr(is_vtype_vec<std::remove_pointer_t<typename To::element_type>, DB::UUID> ) {
                if (from && typeid_cast_colvec<DB::UUID>(*from, try_flush_zerocpy_buf)) 
                    return static_pointer_cast<typename To::element_type>(from);
            }
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::UInt8>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::UInt16>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::UInt32>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::UInt64>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::UInt128>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::UInt256>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::Int8>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::Int16>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::Int32>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::Int64>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::Int128>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::Int256>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::Float32>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::Float64>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  else if constexpr(is_same_vtype_vec<std::remove_pointer_t<typename To::element_type>, From, DB::UUID>) {
            return static_pointer_cast<typename To::element_type>(from);
        }  

        if ((typeid(From) == typeid(typename To::element_type)) || (from && typeid(*from) == typeid(typename To::element_type)))
            return std::static_pointer_cast<typename To::element_type>(from);
        else
            return nullptr;
    }
    catch (const std::exception & e)
    {
        throw DB::Exception(e.what(), DB::ErrorCodes::LOGICAL_ERROR);
    }
}
