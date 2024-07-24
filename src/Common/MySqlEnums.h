#pragma once

namespace DB
{
enum class ArgType
{
    STRINGS,                /// (String/FixedString, ...)
    NUMBERS,                /// (Number a, Number b, ...) Numbers include int, uint, float and decimal.
    NOT_DECIMAL,            /// (Native number including int. uint, float but excluding decimal)
    UINTS,                  /// (UInt a, UInt b, ...)
    DATES,                  /// (General Date/Datetime Date a, Date b, ...) Date include date, datetime, datetime64, time
    DATE_OR_DATETIME,       /// (Date or DateTime only Date a, Date b, ...)
    NUM_STR,                /// (Number a, String b, String c ...)
    NUM_NUM_STR,            /// (Number a, Number b, String c)
    INT_STR,                /// (Int a, String b, ...)
    INTS,                   /// (Int a, Int b, ...)
    STR_NUM,                /// (String a, Number b)
    STR_NUM_NUM_STR,        /// (String a, Number b, Number c, String d)
    STR_NUM_STR,            /// (String a, Number b, String c)
    STR_UINT_STR,           /// (String a, UInt b, String c)
    STR_STR_NUM,            /// (String a, String b, Number c)
    STR_STR_UINT,           /// (String a, String b, UInt c)
    SAME_TYPE,              /// Two arguments of same type
    DATE_NUM_STR,           /// (General Date/Datetime type a, Number b, String c)
    DATE_INTERVAL,          /// (Date/DateTime a, Interval b)
    INTERVAL_DATE,          /// (Interval a, Date/DateTime b)
    DATE_STR,               /// (General Date/Datetime type a, String b)
    STR_DATETIME_STR,       /// (String a, General Date/Datetime b,  General Date/Datetime c, String d)
    ARRAY_FIRST,            /// (Array<T> a, T b, T c...)
    ARRAY_COMMON,           /// (Array<T> a, Array<T> b)
    UNDEFINED               /// All MySql functions should override getArgumentsType()
};
}
