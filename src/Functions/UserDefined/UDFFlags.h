#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <string.h>
#include <Common/BitHelpers.h>

namespace DB {

using UDFFlags = uint32_t;


// Users can specify the flags value. Currently, we reserve the last 7 bits from 25-31 for
// internal representation of user specified udf language and type.
static inline int getEnumValFromString(const char* enum_name, const char * const * arr, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        if (!strcasecmp(arr[i], enum_name))
            return i;
    }
    return -1;
}

enum class UDFLanguage
{
    Python,
    SQL
};
static const char* Languages[] = {"Python", "SQL"};

enum class UDFFunctionType
{
    Scalar,
    Aggregate,
    Count,
};

static const char* FunctionTypes[] = {"Scalar", "Aggregate"};

enum class UDFFlag {
    UseDefaultImplementationForLowCardinalityColumns, /* enable always. convert all low cardinality columsn to ordinary column */
    UseDefaultImplementationForConstants,             /* enable always. convert to ordinary column if all arguments are constant */
    UseDefaultImplementationForNulls,                 /* disable by default. peel off nullable and use nested column */
    SuitableForShortCircuitArgumentsExecution,        /* disable by default. evaluate this function lazily in short-circuit function arguments */
    SuitableForConstantFolding,                       /* disable by default. enable early execution if all arguments are constant */
    CanBeExecutedOnLowCardinalityDictionary,          /* disable always. not support for now */
    CanBeExecutedOnDefaultArguments,                  /* enable by default. conterexample: modulo(0,0) */
    DeterministicInScopeOfQuery,                      /* disable by default. return same result for same values during single query. example: now() */
    Deterministic,                                    /* disable by default. return same result for same values. counterexample: rand() */
    AlwaysPositiveMonotonicity,                       /* disable by default. */
    AlwaysNegativeMonotonicity,                       /* disable by default. */
    Injective,                                        /* disable by default. func should return unique result if inputs are different. optimization for group by */
    Stateful,                                         /* disable by default. for pushdown and merge optimization */
    MaxFunctionFlagCnt,

    FunctionType = 25,                                /* 25 - 26 bits represent the type of functions, see enum UDFFunctionType. Default is Scalar */
    Language = 28,                                    /* 28 - 30 bits represent the language, see enum UDFLanguage. Default is SQL */
};

static_assert(UDFFlag::MaxFunctionFlagCnt <= UDFFlag::FunctionType,
              "UDF Flags can't fit in 32 bits");

#define UDF_FIELD_TYPE GENMASK(26, 25)
#define UDF_FIELD_LANG GENMASK(30, 28)
#define USER_MAX_FLAG_VALUE GENMASK(24, 0)

static inline const UDFFlags SCALAR_UDF_DEFAULT_FLAGS =
    BIT(UDFFlag::UseDefaultImplementationForLowCardinalityColumns) |
    BIT(UDFFlag::UseDefaultImplementationForConstants) |
    BIT(UDFFlag::CanBeExecutedOnDefaultArguments);

static inline enum UDFLanguage getLanguage(UDFFlags flags)
{
    auto lang = FIELD_GET(UDF_FIELD_LANG, flags);
    return static_cast<UDFLanguage>(lang);
}

static inline enum UDFFunctionType getFunctionType(UDFFlags flags)
{
    return static_cast<UDFFunctionType>(FIELD_GET(UDF_FIELD_TYPE, flags));
}

static inline const char * getFunctionTypeStr(UDFFunctionType func_type)
{
    return FunctionTypes[to_underlying(func_type)];
}

static inline const char * getLanguageStr(UDFLanguage language)
{
    return Languages[to_underlying(language)];
}

static inline bool setFlag(UDFFlags &flags, const char *value, const char** enums, size_t n, uint64_t mask)
{
    int z = getEnumValFromString(value, enums, n);
    if (z == -1)
        return false;

    flags |= FIELD_PREP(mask, z);
    return true;
}

static inline bool setLangauge(UDFFlags &flags, const char *language)
{
    return setFlag(flags, language, Languages, sizeof(Languages), UDF_FIELD_LANG);
}

static inline bool setFunctionType(UDFFlags &flags, const char *type)
{
    return setFlag(flags, type, FunctionTypes, sizeof(FunctionTypes), UDF_FIELD_TYPE);
}


static inline uint32_t getUserDefinedFlags(uint32_t flags)
{
    return FIELD_GET(USER_MAX_FLAG_VALUE, flags);
}

}
