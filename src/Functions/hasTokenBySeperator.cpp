#include <unordered_set>
#include <Functions/IFunction.h>
#include <Common/PODArray_fwd.h>
#include <common/types.h>
#include <Common/register_objects.h>
#include <Common/typeid_cast.h>
#include <Common/Volnitsky.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionHasTokenBySeperator: public IFunction
{
public:
    static constexpr auto name = "hasTokenBySeperator";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionHasTokenBySeperator>();
    }

    String getName() const override
    {
        return name;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName& arguments, const DataTypePtr&,
        size_t input_rows_count) const override
    {
        const ColumnPtr& raw_haystack_col = arguments[0].column;
        const ColumnPtr& raw_needle_col = arguments[1].column;
        const ColumnPtr& raw_seperators_col = arguments[2].column;

        const ColumnString* haystack_col = typeid_cast<const ColumnString*>(
            &(*raw_haystack_col));
        const ColumnConst* const_needle_col = typeid_cast<const ColumnConst*>(
            &(*raw_needle_col));
        const ColumnConst* const_seperators_col = typeid_cast<const ColumnConst*>(
            &(*raw_seperators_col));

        if (const_needle_col == nullptr || const_seperators_col == nullptr)
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Needle column and seperator "
                "column must be const");
        }

        String needle_str = const_needle_col->getValue<String>();
        String seperators_str = const_seperators_col->getValue<String>();
        std::unordered_set<char> seperators_set(seperators_str.begin(),
            seperators_str.end());

        if (needle_str.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Needle is empty");
        }
        if (bool needle_has_seperator = std::any_of(needle_str.begin(), needle_str.end(), [&](char c) {
            return seperators_set.contains(c);
        }); needle_has_seperator)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Needle {} contains seperator {}",
                needle_str, seperators_str);
        }

        auto res_col = ColumnUInt8::create(input_rows_count, 0);

        if (haystack_col->empty())
        {
            return res_col;
        }

        const ColumnString::Chars& haystack_data = haystack_col->getChars();
        const ColumnString::Offsets& haystack_offsets = haystack_col->getOffsets();
        const UInt8 * const begin = haystack_data.data();
        const UInt8 * const end = haystack_data.data() + haystack_data.size();
        const UInt8 * pos = begin;
        size_t needle_size = needle_str.size();

        Volnitsky searcher(needle_str.data(), needle_size, end - pos);

        PaddedPODArray<UInt8>& res = res_col->getData();
        /// The current index in the array of strings.
        size_t i = 0;
        /// We will search for the next occurrence in all rows at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// The found substring is a token
            if ((pos == begin || isTokenSeparator(seperators_set, pos[-1]))
                && (pos + needle_size == end || isTokenSeparator(seperators_set, pos[needle_size])))
            {
                /// Let's determine which index it refers to.
                while (begin + haystack_offsets[i] <= pos)
                {
                    res[i] = false;
                    ++i;
                }

                /// We check that the entry does not pass through the boundaries of strings.
                res[i] = (pos + needle_str.size() < begin + haystack_offsets[i]);

                pos = begin + haystack_offsets[i];
                ++i;
            }
            else
            {
                /// Not a token. Jump over it.
                pos += needle_size;
            }
        }

        /// Tail, in which there can be no substring.
        if (i < res.size())
            memset(&res[i], false, (res.size() - i) * sizeof(res[0]));

        return res_col;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return {1, 2};
    }

    size_t getNumberOfArguments() const override
    {
        return 3;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override
    {
        if (arguments.size() != 3)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} need 3 arguments got {}", getName(), arguments.size());
        }

        for (size_t i = 0; i < 3; ++i)
        {
            if (!isStringOrFixedString(arguments[i]))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of "
                    "argument {} for function {}", arguments[i]->getName(),
                    i, getName());
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    static inline bool isTokenSeparator(const std::unordered_set<char>& seperators,
        char c)
    {
        return !isPrintableASCII(c) || seperators.contains(c);
    }
};

}

REGISTER_FUNCTION(HasTokenBySeperator)
{
    factory.registerFunction<FunctionHasTokenBySeperator>(FunctionFactory::CaseSensitive);
}

}
