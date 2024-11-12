/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <algorithm>
#include <array>
#include <stack>

#include <DataTypes/DataTypeBitMap64.h>

#include <common/types.h>
#include <Common/ArenaAllocator.h>
#include <Columns/ListIndex.h>
#include <AggregateFunctions/AggregateBitmapExpression_fwd.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

#define AGGREGATE_INTEGRAL_BITMAP_KEY_INTERNAL_START (std::numeric_limits<Int64>::min()+10000)
#define AGGREGATE_STRING_BITMAP_KEY_INTERNAL_PREFIX "BITMAP*AGG*KEY"

template<typename T>
inline T trans_global_index_to_bitmap_key(int arg)
{
    return AGGREGATE_INTEGRAL_BITMAP_KEY_INTERNAL_START+arg;
}

template<>
inline String trans_global_index_to_bitmap_key(int arg)
{
    return AGGREGATE_STRING_BITMAP_KEY_INTERNAL_PREFIX+toString(arg);
}

template<typename T>
inline String trans_global_index_to_bitmap_string_key(int arg)
{
    return toString(AGGREGATE_INTEGRAL_BITMAP_KEY_INTERNAL_START+arg);
}

template<>
inline String trans_global_index_to_bitmap_string_key<String>(int arg)
{
    return AGGREGATE_STRING_BITMAP_KEY_INTERNAL_PREFIX+toString(arg);
}

template<typename T>
inline bool check_is_internal_bitmap_key(T key)
{
    return (static_cast<Int64>(key) < AGGREGATE_INTEGRAL_BITMAP_KEY_INTERNAL_START);
}

template<>
inline bool check_is_internal_bitmap_key(String key)
{
    return 0 == key.compare(0, strlen(AGGREGATE_STRING_BITMAP_KEY_INTERNAL_PREFIX), AGGREGATE_STRING_BITMAP_KEY_INTERNAL_PREFIX);
}

inline bool existBitengineExpressionKeyword(const String& str)
{
    if (str.empty())
        return false;

    for (const char & keyword : BITENGINE_EXPRESSION_KEYWORDS)
    {
        if (str.find(keyword) != std::string::npos)
        {
            return true;
        }
    }

    if (!str.empty() && str[0] == BITENGINE_SPECIAL_KEYWORD)
    {
        throw Exception("Input tag starts with '_' is not allowd. your tag: " + str
        , ErrorCodes::BAD_ARGUMENTS);
    }

    return false;
}

template <typename T>
void checkIntegerExpression(const String & expression)
{
    auto is_integer = [&]() {
        try
        {
            std::size_t pos;
            [[maybe_unused]] UInt64 result = std::stoull(expression, &pos);

            return pos == expression.size();
        }
        catch (...)
        {
            return false;
        }
    };

    if constexpr (std::is_integral_v<T>)
    {
        if (is_integer())
        {
            return;
        }

        if (expression.find('-') != expression.npos)
        {
            throw Exception(
                "The tag (or bitmap key): " + expression + " has character '-', "
                    + "you mean to calculate the difference set? If so, you should use '~', but not '-'.",
                ErrorCodes::BAD_ARGUMENTS);
        }
        else if (expression.find("～") != expression.npos)
        {
            throw Exception(
                "The tag (or bitmap key): " + expression + " has Chinese character '～', "
                    + "you mean to calculate the difference set? If so, you should use English character '~'.",
                ErrorCodes::BAD_ARGUMENTS);
        }
        else if (expression.find("，") != expression.npos)
        {
            throw Exception(
                "The tag (or bitmap key): " + expression + " has Chinese character '，', "
                    + "you mean to calculate the union set? If so, you should use English character ','.",
                ErrorCodes::BAD_ARGUMENTS);
        }
        else if (expression.find("｜") != expression.npos)
        {
            throw Exception(
                "The tag (or bitmap key): " + expression + " has Chinese character '｜', "
                    + "you mean to calculate the union set? If so, you should use English character '|'.",
                ErrorCodes::BAD_ARGUMENTS);
        }
        else
        {
            throw Exception(
                "The tag (or bitmap key): " + expression + " is illegal, " + "and it should be an integer.", ErrorCodes::BAD_ARGUMENTS);
        }
    }
}

template<typename T,
          typename BitmapType,
    typename = std::enable_if_t< (std::is_integral_v<T> || std::is_same_v<T, String>) && (std::is_same_v<BitmapType, BitMap64> || std::is_same_v<BitmapType, BitMap>) > >
struct AggregateFunctionBitMapDataImpl
{
    std::unordered_map<T, BitmapType> bitmap_map;
    bool is_finished = false;
    AggregateFunctionBitMapDataImpl() = default;

    void add(const T key, const BitmapType & bitmap)
    {
        auto it = bitmap_map.find(key);
        if (it == bitmap_map.end()) {
            bitmap_map.emplace(key, bitmap);
        } else {
            it->second |= bitmap;
        }
    }

    void merge(AggregateFunctionBitMapDataImpl<T, BitmapType> && rhs)
    {
        std::unordered_map<T, BitmapType> & rhs_map = rhs.bitmap_map;
        for (auto it = rhs_map.begin(); it != rhs_map.end(); ++it)
        {
            auto jt = bitmap_map.find(it->first);
            if (jt == bitmap_map.end()) {
                bitmap_map.emplace(it->first, it->second);
            }
            else {
                jt->second |= it->second;
            }
        }
        is_finished = false;
    }

    bool empty() const { return bitmap_map.empty(); }

    UInt64 getCardinality(const T & key)
    {
        auto it = bitmap_map.find(key);
        if (it != bitmap_map.end())
            return it->second.cardinality();
        return 0;
    }

    void serialize(WriteBuffer & buf) const
    {
        size_t key_size = bitmap_map.size();
        writeVarUInt(key_size, buf);

        for (auto it = bitmap_map.begin(); it != bitmap_map.end(); ++it)
        {
            writeBinary(it->first, buf);
            size_t bytes_size = it->second.getSizeInBytes();
            writeVarUInt(bytes_size, buf);
            PODArray<char> buffer(bytes_size);
            it->second.write(buffer.data());
            writeString(buffer.data(), bytes_size, buf);
        }

        writeVarUInt(is_finished, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t key_size;
        readVarUInt(key_size, buf);
        for (size_t i = 0; i < key_size; ++i)
        {
            T key;
            readBinary(key, buf);
            size_t bytes_size;
            readVarUInt(bytes_size, buf);
            PODArray<char> buffer(bytes_size);
            buf.readStrict(buffer.data(), bytes_size);
            BitmapType bitmap = BitmapType::readSafe(buffer.data(), bytes_size);
            bitmap_map.emplace(key, std::move(bitmap));
        }

        readVarUInt(is_finished, buf);
    }
};


template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
using AggregateFunctionBitMapData = AggregateFunctionBitMapDataImpl<T, BitMap64>;


template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
struct BitMapExpressionNode
{
    String left;
    String op;
    String right;
    T res;
    bool replicated = false;
    BitMapExpressionNode(const String & left_, const String & op_, const String & right_, const T res_, bool replicated_ = false)
    : left(left_), op(op_), right(right_), res(res_), replicated(replicated_) {}

    BitMapExpressionNode(String && left_, String && op_, String && right_, const T res_, bool replicated_ = false)
    : left(std::move(left_)), op(std::move(op_)), right(std::move(right_)), res(res_), replicated(replicated_) {}

    String toString() const
    {
        std::ostringstream oss;
        oss << left << " " << op << " " << right << " = " << res << " REPLICATED: " << replicated << "\n";
        return oss.str();
    }
};

template<typename T,
          typename BitmapType,
          typename = std::enable_if_t< (std::is_integral_v<T> || std::is_same_v<T, String>) && (std::is_same_v<BitmapType, BitMap64> || std::is_same_v<BitmapType, BitMap>) > >
struct BitMapExpressionAnalyzerImpl
{
    using BitMapExpressions = std::vector<BitMapExpressionNode<T>>;
    String original_expression;
    T final_key;
    BitMapExpressions expression_actions;
    std::unordered_map<String, size_t> replicated_keys;
    bool only_or = true;
    NameSet or_expressions;

    explicit BitMapExpressionAnalyzerImpl(const String & expression)
    : original_expression(expression)
    {
        analyze();
    }
    BitMapExpressionAnalyzerImpl() = default;

    void subExpression(std::stack<String> & expression_stack, int & global_index, String & right)
    {
        while (!expression_stack.empty() &&
            (expression_stack.top() == "&" || expression_stack.top() == "|"
            || expression_stack.top() == "," || expression_stack.top() == "~" || right == "#"))
        {
            if (right == "#")
            {
                right = expression_stack.top();
                expression_stack.pop();
            }
            if (expression_stack.empty())
                break;
            String operation = expression_stack.top();
            expression_stack.pop();
            if (expression_stack.empty())
                throw Exception("Invalid expression " + operation + " for BitMap: " + original_expression, ErrorCodes::BAD_ARGUMENTS);
            String left = expression_stack.top();
            expression_stack.pop();
            // Optimize the case which right is equal to left.
            // If the operation is "~", add a no-exists result to expression_stack so that we can get an empty bitmap
            if (right == left && operation == "~")
            {
                int res = global_index--;
                right = trans_global_index_to_bitmap_string_key<T>(res);
            }
            else if (right != left)
            {
                int res = global_index--;
                bool replicated = false;
                auto left_it = replicated_keys.find(left);
                auto right_it = replicated_keys.find(right);
                if (left_it != replicated_keys.end())
                {
                    if (left_it->second > 1)
                        replicated = true;
                }
                if (right_it != replicated_keys.end())
                {
                    if (right_it->second > 1)
                        replicated = true;
                }
                expression_actions.emplace_back(
                    std::move(left), std::move(operation), std::move(right), trans_global_index_to_bitmap_key<T>(res), replicated);
                right = trans_global_index_to_bitmap_string_key<T>(res);
            }
        }
    }

    void analyze()
    {
        String expression = original_expression + "#";
        std::stack<String> expression_stack;
        size_t expression_size = expression.size();
        std::vector<String> expression_vector;
        size_t number_index = expression_size;
        for (size_t i = 0; i < expression_size; i++)
        {
            if (expression[i] == '(' || expression[i] == '&' || expression[i] == '|' || expression[i] == ','
            || expression[i] == ')' || expression[i] == '#' || expression[i] == '~' || expression[i] == ' ') {
                if (number_index != expression_size) {
                    String number = expression.substr(number_index, (i - number_index));
                    if constexpr (std::is_integral_v<T>)
                    {
                        checkIntegerExpression<T>(number);
                    }
                    replicated_keys[number] += 1;
                    or_expressions.insert(number);
                    expression_vector.push_back(std::move(number));
                    number_index = expression_size;
                }
                switch (expression[i]) {
                    case '(':
                        expression_vector.push_back("(");
                        break;
                    case '&':
                    {
                        expression_vector.push_back("&");
                        only_or = false;
                        break;
                    }
                    case '|':
                        expression_vector.push_back("|");
                        break;
                    case ')':
                        expression_vector.push_back(")");
                        break;
                    case ',':
                        expression_vector.push_back(",");
                        break;
                    case '~':
                    {
                        expression_vector.push_back("~");
                        only_or = false;
                        break;
                    }
                    case '#':
                        expression_vector.push_back("#");
                        break;
                }
            } else {
                if (number_index == expression_size) {
                    number_index = i;
                }
            }
        }

        if (only_or)
        {
            final_key = trans_global_index_to_bitmap_key<T>(-1);
            return;
        }

        int global_index = -1;

        for (const auto & i : expression_vector)
        {
            if (i == "(" || i == "&"
                || i == "|" || i == ","
                || i == "~")
            {
                expression_stack.push(i);
            }
            else if (i == ")")
            {
                if (expression_stack.empty())
                    throw Exception("Invalid expression " + i + " for BitMap: " + original_expression, ErrorCodes::BAD_ARGUMENTS);
                String number = expression_stack.top();
                expression_stack.pop();
                if (expression_stack.empty())
                    throw Exception("Invalid expression " + number + " for BitMap: " + original_expression, ErrorCodes::BAD_ARGUMENTS);
                expression_stack.pop();
                subExpression(expression_stack, global_index, number);
                expression_stack.push(number);
            }
            else
            {
                String right = i;
                // If there are replicated number, we cannot use some optimization strategy to execute expression
                subExpression(expression_stack, global_index, right);
                expression_stack.push(right);
            }
        }

        if (expression_stack.size() == 1) {
            const String & res = expression_stack.top();
            std::istringstream iss(res);
            iss >> final_key;
        } else {
            throw Exception("Invalid expression for BitMap: " + original_expression, ErrorCodes::BAD_ARGUMENTS);
        }
    }

    void executeExpressionImpl(String left, String operation, String right, T res, bool replicated, const AggregateFunctionBitMapDataImpl<T, BitmapType>& data) const
    {
        std::istringstream iss(left);
        T left_key;
        iss >> left_key;
        std::istringstream iss2(right);
        T right_key;
        iss2 >> right_key;

        auto& bitmap_map = const_cast<std::unordered_map<T, BitmapType>&>(data.bitmap_map);
        auto left_iter = bitmap_map.find(left_key);
        auto right_iter = bitmap_map.find(right_key);

        if (left_iter == bitmap_map.end()) {
            BitmapType temp_bitmap;
            auto res_pair = bitmap_map.emplace(left_key, std::move(temp_bitmap));
            if (res_pair.second)
                left_iter = res_pair.first;
            else
                throw Exception("Existing empty BitMap64 when inserting empty BitMap64", ErrorCodes::LOGICAL_ERROR);
        }
        if (right_iter == bitmap_map.end()) {
            BitmapType temp_bitmap;
            auto res_pair = bitmap_map.emplace(right_key, std::move(temp_bitmap));
            if (res_pair.second)
                right_iter = res_pair.first;
            else
                throw Exception("Existing empty BitMap64 when inserting empty BitMap64", ErrorCodes::LOGICAL_ERROR);
        }
        if (!replicated)
        {
            if (operation == "|" || operation == ",") {
                left_iter->second |= right_iter->second;
                auto left_item = bitmap_map.extract(left_iter->first);
                left_item.key() = res;
                bitmap_map.insert(std::move(left_item));
            }
            else if (operation == "&") {
                left_iter->second &= right_iter->second;
                auto left_item = bitmap_map.extract(left_iter->first);
                left_item.key() = res;
                bitmap_map.insert(std::move(left_item));
            }
            else if (operation == "~") {
                left_iter->second -= right_iter->second;
                auto left_item = bitmap_map.extract(left_iter->first);
                left_item.key() = res;
                bitmap_map.insert(std::move(left_item));
            }
        }
        else
        {
            if (operation == "|" || operation == ",") {
                bitmap_map[res] = left_iter->second;
                bitmap_map[res] |= right_iter->second;
            }
            else if (operation == "&") {
                bitmap_map[res] = left_iter->second;
                bitmap_map[res] &= right_iter->second;
            }
            else if (operation == "~") {
                bitmap_map[res] = left_iter->second;
                bitmap_map[res] -= right_iter->second;
            }
        }
    }

    void executeExpressionOnlyOr(const AggregateFunctionBitMapDataImpl<T, BitmapType> & data) const
    {
        std::set<T> key_set;
        for (const auto & expression : or_expressions)
        {
            std::istringstream iss(expression);
            T key;
            iss >> key;
            key_set.insert(key);
        }

        auto& bitmap_map = const_cast<std::unordered_map<T, BitmapType>&>(data.bitmap_map);

        if (key_set.size() == 1)
        {
            T key = *key_set.begin();
            auto it = bitmap_map.find(key);
            if (it == bitmap_map.end()) {
                BitmapType temp_bitmap;
                auto res_pair = bitmap_map.emplace(key, std::move(temp_bitmap));
                if (res_pair.second)
                    it = res_pair.first;
                else
                    throw Exception("Existing empty BitMap64 when inserting empty BitMap64", ErrorCodes::LOGICAL_ERROR);
            }
            auto it_final_item = bitmap_map.extract(it->first);
            it_final_item.key() = final_key;
            bitmap_map.insert(std::move(it_final_item));
            return;
        }

        if constexpr(std::is_same_v<BitmapType, BitMap64>)
        {
            std::map<UInt32, std::vector<roaring::Roaring*>> roaring_map;
            for (const auto & key: key_set)
            {
                auto it = bitmap_map.find(key);
                if (it == bitmap_map.end())
                    continue;
                std::map<UInt32, roaring::Roaring> & inner_roaring = const_cast<std::map<UInt32, roaring::Roaring> &>(it->second.getRoarings());
                for (auto & jt : inner_roaring)
                {
                    if (roaring_map.find(jt.first) == roaring_map.end())
                        roaring_map.emplace(jt.first, std::vector<Roaring *>());
                    roaring_map[jt.first].emplace_back(&jt.second);
                }
            }

            BitMap64 res_roaring;

            for (auto & it : roaring_map)
            {
                roaring::Roaring result = roaring::Roaring::fastunion(it.second.size(), static_cast<roaring::Roaring **>(&(*(it.second.begin()))));
                const_cast<std::map<UInt32, roaring::Roaring> &>(res_roaring.getRoarings()).emplace(it.first, std::move(result));
            }

            bitmap_map[final_key] = std::move(res_roaring);
        }
        else if constexpr (std::is_same_v<BitmapType, BitMap>)
        {
            std::vector<roaring::Roaring*> vec;
            for (const auto & key: key_set)
            {
                auto it = bitmap_map.find(key);
                if (it == bitmap_map.end())
                    continue;
                vec.emplace_back(reinterpret_cast<roaring::Roaring*>(&(it->second)));
            }
            roaring::Roaring res_roaring = roaring::Roaring::fastunion(vec.size(), &(*(vec.begin())));
            bitmap_map[final_key].loadBitmap(std::move(res_roaring));
        }

    }

    void executeExpression(const AggregateFunctionBitMapDataImpl<T, BitmapType> & data) const
    {
        if (only_or)
        {
            executeExpressionOnlyOr(data);
        }
        else
        {
            for (const auto & action : expression_actions)
            {
                executeExpressionImpl(action.left, action.op, action.right, action.res, action.replicated, data);
            }
        }
    }
};
template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
using BitMapExpressionAnalyzer = BitMapExpressionAnalyzerImpl<T, BitMap64>;


template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
struct BitMapExpressionMultiAnalyzer
{
    using BitMapExpressions = std::vector<BitMapExpressionNode<T>>;
    std::vector<String> original_expressions;
    int global_index = -1;
    std::vector<T> final_keys;
    std::vector<bool> expression_only_ors;
    std::vector<BitMapExpressions> expression_actions_vector;
    std::unordered_map<String, size_t> replicated_keys;
    std::vector<NameSet> or_expressions;

    explicit BitMapExpressionMultiAnalyzer(const std::vector<String> & expressions)
            : original_expressions(expressions)
    {
        analyze();
    }

    BitMapExpressionMultiAnalyzer() = default;

    void subExpression(std::stack<String> & expression_stack, String & right, size_t index)
    {
        while (!expression_stack.empty() &&
               (expression_stack.top() == "&" || expression_stack.top() == "|"
                || expression_stack.top() == "," || expression_stack.top() == "~" || right == "#"))
        {
            if (right == "#")
            {
                right = expression_stack.top();
                expression_stack.pop();
            }
            if (expression_stack.empty())
                break;
            String operation = expression_stack.top();
            expression_stack.pop();
            if (expression_stack.empty())
                throw Exception("Invalid expression " + operation + " for BitMap: " + original_expressions[index], ErrorCodes::BAD_ARGUMENTS);
            String left = expression_stack.top();
            expression_stack.pop();
            // Optimize the case which right is equal to left.
            // If the operation is "~", add a no-exists result to expression_stack so that we can get an empty bitmap
            if (right == left && operation == "~")
            {
                int res = global_index--;
                right = toString(res);
            }
            else if (right != left)
            {
                T res = trans_global_index_to_bitmap_key<T>(global_index--);
                expression_actions_vector[index].emplace_back(std::move(left), std::move(operation), std::move(right), res, false);
                right = toString(res);
            }
        }
    }

    void analyze()
    {
        for (size_t i = 0; i < original_expressions.size(); i++)
            analyzeExpression(original_expressions[i], i);

        for (const auto only_or : expression_only_ors)
        {
            if (only_or)
            {
                for (const NameSet& or_expression: or_expressions)
                {
                    for (const String& or_expression_item: or_expression)
                    {
                        replicated_keys[or_expression_item] += 1;
                    }
                }

            }
            else
            {
                for (auto & expression_actions: expression_actions_vector)
                {
                    for (BitMapExpressionNode<T> & expression_action: expression_actions)
                    {
                        replicated_keys[expression_action.left] += 1;
                        replicated_keys[expression_action.right] += 1;
                    }
                }
            }
        }

        for (auto & expression_actions: expression_actions_vector)
        {
            for (BitMapExpressionNode<T> & expression_action: expression_actions)
            {
                auto left_it = replicated_keys.find(expression_action.left);
                do {
                    if (left_it == replicated_keys.end())
                        break;
                    if (left_it->second <= 1)
                        break;

                    expression_action.replicated = true;
                } while(false);
            }
        }
    }
    void analyzeExpression(String& original_expression, size_t index)
    {
        bool only_or = true;
        NameSet or_expression;
        String expression = original_expression + "#";
        std::stack<String> expression_stack;
        size_t expression_size = expression.size();
        std::vector<String> expression_vector;
        size_t number_index = expression_size;
        for (size_t i = 0; i < expression_size; i++)
        {
            if (expression[i] == '(' || expression[i] == '&' || expression[i] == '|' || expression[i] == ','
                || expression[i] == ')' || expression[i] == '#' || expression[i] == '~' || expression[i] == ' ') {
                if (number_index != expression_size) {
                    String number = expression.substr(number_index, (i - number_index));
                    // replace number with final key
                    if (number.size() > 1 && number[0] == '_')
                    {
                        size_t res_index{0};

                        try
                        {
                            res_index = std::stoi(number.substr(1));
                        }
                        catch (std::exception &e)
                        {
                            throw Exception("Bad cast number to position: " + number + ", reason: " + String(e.what()),
                                ErrorCodes::BAD_ARGUMENTS);
                        }

                        if (res_index <= 0 || res_index > index)
                        {
                            throw Exception("Invalid expression " + number + " for BitMap: " + original_expression, ErrorCodes::BAD_ARGUMENTS);
                        }
                        number = toString(final_keys[res_index - 1]);
                    }

                    if constexpr (std::is_integral_v<T>)
                    {
                        checkIntegerExpression<T>(number);
                    }
                    or_expression.insert(number);
                    expression_vector.emplace_back(std::move(number));
                    number_index = expression_size;
                }
                switch (expression[i]) {
                    case '(':
                        expression_vector.emplace_back("(");
                        break;
                    case '&':
                    {
                        expression_vector.emplace_back("&");
                        only_or = false;
                        break;
                    }
                    case '|':
                        expression_vector.emplace_back("|");
                        break;
                    case ')':
                        expression_vector.emplace_back(")");
                        break;
                    case ',':
                        expression_vector.emplace_back(",");
                        break;
                    case '~':
                    {
                        expression_vector.emplace_back("~");
                        only_or = false;
                        break;
                    }
                    case '#':
                        expression_vector.emplace_back("#");
                        break;
                }
            } else {
                if (number_index == expression_size) {
                    number_index = i;
                }
            }
        }
        BitMapExpressions expressions;
        expression_actions_vector.emplace_back(std::move(expressions));
        or_expressions.emplace_back(std::move(or_expression));
        expression_only_ors.emplace_back(only_or);
        if (only_or)
        {
            T current_key = trans_global_index_to_bitmap_key<T>(global_index--);
            final_keys.emplace_back(current_key);
            return;
        }

        for (const auto & i : expression_vector)
        {
            if (i == "(" || i == "&"
                || i == "|" || i == ","
                || i == "~")
            {
                expression_stack.push(i);
            }
            else if (i == ")")
            {
                if (expression_stack.empty())
                    throw Exception("Invalid expression " + i + " for BitMap: " + original_expression, ErrorCodes::BAD_ARGUMENTS);
                String number = expression_stack.top();
                expression_stack.pop();
                if (expression_stack.empty())
                    throw Exception("Invalid expression " + number + " for BitMap: " + original_expression, ErrorCodes::BAD_ARGUMENTS);
                expression_stack.pop();
                subExpression(expression_stack, number, index);
                expression_stack.push(number);
            }
            else
            {
                String right = i;
                // If there are replicated number, we cannot use some optimization strategy to execute expression
                subExpression(expression_stack, right, index);
                expression_stack.push(right);
            }
        }

        if (expression_stack.size() == 1) {
            T temp_final_key;
            const String & res = expression_stack.top();
            std::istringstream iss(res);
            iss >> temp_final_key;
            final_keys.emplace_back(temp_final_key);
        } else {
            throw Exception("Invalid expression for BitMap: " + original_expression, ErrorCodes::BAD_ARGUMENTS);
        }
    }

    void executeExpressionImpl(String left, String operation, String right, T res, bool replicated, const AggregateFunctionBitMapData<T>& data) const
    {
        std::istringstream iss(left);
        T left_key;
        iss >> left_key;
        std::istringstream iss2(right);
        T right_key;
        iss2 >> right_key;

        auto& bitmap_map = const_cast<std::unordered_map<T, BitMap64>&>(data.bitmap_map);
        auto left_iter = bitmap_map.find(left_key);
        auto right_iter = bitmap_map.find(right_key);

        if (left_iter == bitmap_map.end()) {
            BitMap64 temp_bitmap;
            auto res_pair = bitmap_map.emplace(left_key, std::move(temp_bitmap));
            if (res_pair.second)
                left_iter = res_pair.first;
            else
                throw Exception("Existing empty BitMap64 when inserting empty BitMap64", ErrorCodes::LOGICAL_ERROR);
        }
        if (right_iter == bitmap_map.end()) {
            BitMap64 temp_bitmap;
            auto res_pair = bitmap_map.emplace(right_key, std::move(temp_bitmap));
            if (res_pair.second)
                right_iter = res_pair.first;
            else
                throw Exception("Existing empty BitMap64 when inserting empty BitMap64", ErrorCodes::LOGICAL_ERROR);
        }
        if (!replicated)
        {
            if (operation == "|" || operation == ",") {
                left_iter->second |= right_iter->second;
                auto left_item = bitmap_map.extract(left_iter->first);
                left_item.key() = res;
                bitmap_map.insert(std::move(left_item));
            }
            else if (operation == "&") {
                left_iter->second &= right_iter->second;
                auto left_item = bitmap_map.extract(left_iter->first);
                left_item.key() = res;
                bitmap_map.insert(std::move(left_item));
            }
            else if (operation == "~") {
                left_iter->second -= right_iter->second;
                auto left_item = bitmap_map.extract(left_iter->first);
                left_item.key() = res;
                bitmap_map.insert(std::move(left_item));
            }
        }
        else
        {
            if (operation == "|" || operation == ",") {
                bitmap_map[res] = left_iter->second;
                bitmap_map[res] |= right_iter->second;
            }
            else if (operation == "&") {
                bitmap_map[res] = left_iter->second;
                bitmap_map[res] &= right_iter->second;
            }
            else if (operation == "~") {
                bitmap_map[res] = left_iter->second;
                bitmap_map[res] -= right_iter->second;
            }
        }
    }

    void executeExpressionOnlyOr(const AggregateFunctionBitMapData<T> & data, size_t index) const
    {
        std::set<T> key_set;
        for (const auto & expression : or_expressions[index])
        {
            std::istringstream iss(expression);
            T key;
            iss >> key;
            if (toString(key) != expression)
                throw Exception("expression is not fully parsed! parsed: " + toString(key) + ", but your input: " + expression
                    + ", please check function name and expression type", ErrorCodes::BAD_ARGUMENTS);
            key_set.insert(key);
        }

        auto& bitmap_map = const_cast<std::unordered_map<T, BitMap64>&>(data.bitmap_map);

        if (key_set.size() == 1)
        {
            T key = *key_set.begin();
            auto it = bitmap_map.find(key);
            if (it == bitmap_map.end()) {
                BitMap64 temp_bitmap;
                auto res_pair = bitmap_map.emplace(key, std::move(temp_bitmap));
                if (res_pair.second)
                    it = res_pair.first;
                else
                    throw Exception("Existing empty BitMap64 when inserting empty BitMap64", ErrorCodes::LOGICAL_ERROR);
            }
            auto or_it = replicated_keys.find(toString(key));
            if (or_it == replicated_keys.end())
                return;
            else if (or_it->second > 1)
            {
                bitmap_map[final_keys[index]] = it->second;
            }
            else
            {
                auto it_final_item = bitmap_map.extract(it->first);
                it_final_item.key() = final_keys[index];
                bitmap_map.insert(std::move(it_final_item));
            }
            return;
        }

        std::map<UInt32, std::vector<roaring::Roaring*>> roaring_map;
        for (const auto & key: key_set)
        {
            auto it = bitmap_map.find(key);
            if (it == bitmap_map.end())
                continue;
            std::map<UInt32, roaring::Roaring> & inner_roaring = const_cast<std::map<UInt32, roaring::Roaring> &>(it->second.getRoarings());
            for (auto & jt : inner_roaring)
            {
                if (roaring_map.find(jt.first) == roaring_map.end())
                    roaring_map.emplace(jt.first, std::vector<roaring::Roaring *>());
                roaring_map[jt.first].emplace_back(&jt.second);
            }
        }

        BitMap64 res_roaring;

        for (auto & it : roaring_map)
        {
            roaring::Roaring result = roaring::Roaring::fastunion(it.second.size(), static_cast<roaring::Roaring **>(&(*(it.second.begin()))));
            const_cast<std::map<UInt32, roaring::Roaring> &>(res_roaring.getRoarings()).emplace(it.first, std::move(result));
        }

        bitmap_map[final_keys[index]] = std::move(res_roaring);
    }

    void executeExpression(const AggregateFunctionBitMapData<T> & data, size_t index) const
    {
        if (expression_only_ors[index])
        {
            executeExpressionOnlyOr(data, index);
        }
        else
        {
            for (const auto & action : expression_actions_vector[index])
            {
                executeExpressionImpl(action.left, action.op, action.right, action.res, action.replicated, data);
            }
        }
    }
};


struct BitMapExpressionWithDateMultiAnalyzer
{
    using BitMapExpressions = std::vector<BitMapExpressionNode<String>>;
    // original_expressions is a list extracted from AggFunction's first parameter.
    // for SQL below:
    // * Select BitmapMultiCountWithDateV2('conjunct1','conjunct2')(p_date, tag, uid) From ... *
    // The original_expressions is ['conjunct1', 'conjunct2']
    std::vector<String> original_expressions;
    Int64 global_index = -1;
    std::unordered_set<String> keys_without_date;
    std::vector<String> final_keys;
    std::vector<bool> expression_only_ors;
    std::vector<BitMapExpressions> expression_actions_vector;
    std::unordered_map<String, size_t> replicated_keys;
    std::vector<NameSet> or_expressions;
    std::unordered_set<String> interested_tokens;

    explicit BitMapExpressionWithDateMultiAnalyzer(const std::vector<String> & expressions)
            : original_expressions(expressions)
    {
        analyze();
    }

    BitMapExpressionWithDateMultiAnalyzer() = default;

    void subExpression(std::stack<String> & expression_stack, String & right, size_t index)
    {
        while (!expression_stack.empty() &&
               (expression_stack.top() == "&" || expression_stack.top() == "|"
                || expression_stack.top() == "," || expression_stack.top() == "~" || right == "#"))
        {
            if (right == "#")
            {
                right = expression_stack.top();
                expression_stack.pop();
            }
            if (expression_stack.empty())
                break;
            String operation = expression_stack.top();
            expression_stack.pop();
            if (expression_stack.empty())
                throw Exception("Invalid expression " + operation + " for BitMap: " + original_expressions[index], ErrorCodes::BAD_ARGUMENTS);
            String left = expression_stack.top();
            expression_stack.pop();
            // Optimize the case which right is equal to left.
            // If the operation is "~", add a no-exists result to expression_stack so that we can get an empty bitmap
            if (right == left && operation == "~")
            {
                Int64 res = global_index--;
                right = std::to_string(res);
            }
            else
            {
                Int64 res = global_index--;

                expression_actions_vector[index].emplace_back(std::move(left), std::move(operation), std::move(right), std::to_string(res), false);
                right = std::to_string(res);
            }
        }
    }

    void analyze()
    {
        for (size_t i = 0; i < original_expressions.size(); i++)
            analyzeExpression(original_expressions[i], i);
        for (auto & expression_actions: expression_actions_vector)
        {
            for (BitMapExpressionNode<String> & expression_action: expression_actions)
            {
                replicated_keys[expression_action.left] += 1;
                replicated_keys[expression_action.right] += 1;
            }
        }
        for (const NameSet& or_expression: or_expressions)
        {
            for (const String& or_expression_item: or_expression)
            {
                replicated_keys[or_expression_item] += 1;
            }
        }

        for (auto & expression_actions: expression_actions_vector)
        {
            for (BitMapExpressionNode<String> & expression_action: expression_actions)
            {
                auto left_it = replicated_keys.find(expression_action.left);
                do {
                    if (left_it == replicated_keys.end())
                        break;
                    if (left_it->second <= 1)
                        break;

                    expression_action.replicated = true;
                } while(false);
            }
        }
    }
    void analyzeExpression(String& original_expression, size_t index)
    {
        bool only_or = true;
        NameSet or_expression;
        String expression = original_expression + "#";
        std::stack<String> expression_stack;
        size_t expression_size = expression.size();
        std::vector<String> expression_vector;
        size_t number_index = expression_size;
        for (size_t i = 0; i < expression_size; i++)
        {
            if (expression[i] == '(' || expression[i] == '&' || expression[i] == '|' || expression[i] == ','
                || expression[i] == ')' || expression[i] == '#' || expression[i] == '~' || expression[i] == ' ') {
                if (number_index != expression_size) {
                    String number = expression.substr(number_index, (i - number_index));
                    // check mistakable characters
                    if (number == "-")
                    {
                        throw Exception(
                            "The tag (or bitmap key): " + number + " has character '-', "
                                + "you mean to calculate the difference set? If so, you should use '~', but not '-'.",
                            ErrorCodes::BAD_ARGUMENTS);
                    }
                    else if (number.find("～") != number.npos)
                    {
                        throw Exception(
                            "The tag (or bitmap key): " + number + " has Chinese character '～', "
                                + "you mean to calculate the difference set? If so, you should use English character '~'.",
                            ErrorCodes::BAD_ARGUMENTS);
                    }
                    else if (number.find("，") != number.npos)
                    {
                        throw Exception(
                            "The tag (or bitmap key): " + number + " has Chinese character '，', "
                                + "you mean to calculate the union set? If so, you should use English character ','.",
                            ErrorCodes::BAD_ARGUMENTS);
                    }
                    else if (number.find("｜") != number.npos)
                    {
                        throw Exception(
                            "The tag (or bitmap key): " + number + " has Chinese character '｜', "
                                + "you mean to calculate the union set? If so, you should use English character '|'.",
                            ErrorCodes::BAD_ARGUMENTS);
                    }
                    // replace number with final key
                    if (number.size() > 1 && number[0] == '_')
                    {
                        size_t res_index{0};

                        try {
                            res_index = std::stoi(number.substr(1));
                        } catch (std::exception &e) {
                            throw Exception("Bad cast number to position: " + number + ", reason: " + String(e.what()),
                                ErrorCodes::BAD_ARGUMENTS);
                        }

                        if (res_index <= 0 || res_index > index)
                        {
                            throw Exception("Invalid expression " + number + " for BitMap: " + original_expression, ErrorCodes::BAD_ARGUMENTS);
                        }
                        number = final_keys[res_index - 1];
                    }
                    else if (!number.empty()) {
                        if (number.find('_') == std::string::npos)
                        {
                            keys_without_date.emplace(number);
                        }
                        else
                        {
                            // The expression has '_' in it, So the expression can be a key_date pair.
                            if ('_' != number[0]){
                                // We don't want those _1,_2,... pairs
                                interested_tokens.emplace(number);
                            }
                        }
                    }

                    or_expression.insert(number);
                    expression_vector.push_back(std::move(number));
                    number_index = expression_size;
                }
                switch (expression[i]) {
                    case '(':
                        expression_vector.push_back("(");
                        break;
                    case '&':
                    {
                        expression_vector.push_back("&");
                        only_or = false;
                        break;
                    }
                    case '|':
                        expression_vector.push_back("|");
                        break;
                    case ')':
                        expression_vector.push_back(")");
                        break;
                    case ',':
                        expression_vector.push_back(",");
                        break;
                    case '~':
                    {
                        expression_vector.push_back("~");
                        only_or = false;
                        break;
                    }
                    case '#':
                        expression_vector.push_back("#");
                        break;
                }
            } else {
                if (number_index == expression_size) {
                    number_index = i;
                }
            }
        }
        BitMapExpressions expressions;
        expression_actions_vector.emplace_back(std::move(expressions));
        or_expressions.emplace_back(std::move(or_expression));
        expression_only_ors.emplace_back(only_or);
        if (only_or)
        {
            final_keys.emplace_back(std::to_string(global_index--));
            return;
        }

        for (const auto & i : expression_vector)
        {
            if (i == "(" || i == "&"
                || i == "|" || i == ","
                || i == "~")
            {
                expression_stack.push(i);
            }
            else if (i == ")")
            {
                if (expression_stack.empty())
                    throw Exception("Invalid expression " + i + " for BitMap: " + original_expression, ErrorCodes::BAD_ARGUMENTS);
                String number = expression_stack.top();
                expression_stack.pop();
                if (expression_stack.empty())
                    throw Exception("Invalid expression " + number + " for BitMap: " + original_expression, ErrorCodes::BAD_ARGUMENTS);
                expression_stack.pop();
                subExpression(expression_stack, number, index);
                expression_stack.push(number);
            }
            else
            {
                String right = i;
                // If there are replicated number, we cannot use some optimization strategy to execute expression
                subExpression(expression_stack, right, index);
                expression_stack.push(right);
            }
        }

        if (expression_stack.size() == 1) {
            const String & res = expression_stack.top();
            final_keys.emplace_back(res);
        } else {
            throw Exception("Invalid expression for BitMap: " + original_expression, ErrorCodes::BAD_ARGUMENTS);
        }
    }

    static void executeExpressionImpl(String left_key, String operation, String right_key, String res, bool replicated, const AggregateFunctionBitMapData<String>& data)
    {
        auto& bitmap_map = const_cast<std::unordered_map<String, BitMap64>&>(data.bitmap_map);
        auto left_iter = bitmap_map.find(left_key);
        auto right_iter = bitmap_map.find(right_key);

        if (left_iter == bitmap_map.end()) {
            BitMap64 temp_bitmap;
            auto res_pair = bitmap_map.emplace(left_key, std::move(temp_bitmap));
            if (res_pair.second)
                left_iter = res_pair.first;
            else
                throw Exception("Existing empty BitMap64 when inserting empty BitMap64", ErrorCodes::LOGICAL_ERROR);
        }
        if (right_iter == bitmap_map.end()) {
            BitMap64 temp_bitmap;
            auto res_pair = bitmap_map.emplace(right_key, std::move(temp_bitmap));
            if (res_pair.second)
                right_iter = res_pair.first;
            else
                throw Exception("Existing empty BitMap64 when inserting empty BitMap64", ErrorCodes::LOGICAL_ERROR);
        }
        if (!replicated)
        {
            if (operation == "|" || operation == ",") {
                left_iter->second |= right_iter->second;
                auto left_item = bitmap_map.extract(left_iter->first);
                left_item.key() = res;
                bitmap_map.insert(std::move(left_item));
            }
            else if (operation == "&") {
                left_iter->second &= right_iter->second;
                auto left_item = bitmap_map.extract(left_iter->first);
                left_item.key() = res;
                bitmap_map.insert(std::move(left_item));
            }
            else if (operation == "~") {
                left_iter->second -= right_iter->second;
                auto left_item = bitmap_map.extract(left_iter->first);
                left_item.key() = res;
                bitmap_map.insert(std::move(left_item));
            }
        }
        else
        {
            if (operation == "|" || operation == ",") {
                bitmap_map[res] = left_iter->second;
                bitmap_map[res] |= right_iter->second;
            }
            else if (operation == "&") {
                bitmap_map[res] = left_iter->second;
                bitmap_map[res] &= right_iter->second;
            }
            else if (operation == "~") {
                bitmap_map[res] = left_iter->second;
                bitmap_map[res] -= right_iter->second;
            }
        }
    }

    void executeExpressionOnlyOr(const AggregateFunctionBitMapData<String> & data, size_t index) const
    {
        std::set<String> key_set;
        for (const auto & expression : or_expressions[index])
        {
            key_set.insert(expression);
        }

        auto& bitmap_map = const_cast<std::unordered_map<String, BitMap64>&>(data.bitmap_map);

        if (key_set.size() == 1)
        {
            String key = *key_set.begin();
            auto it = bitmap_map.find(key);
            if (it == bitmap_map.end()) {
                BitMap64 temp_bitmap;
                auto res_pair = bitmap_map.emplace(key, std::move(temp_bitmap));
                if (res_pair.second)
                    it = res_pair.first;
                else
                    throw Exception("Existing empty BitMap64 when inserting empty BitMap64", ErrorCodes::LOGICAL_ERROR);
            }
            auto or_it = replicated_keys.find(key);
            if (or_it == replicated_keys.end())
                return;
            else if (or_it->second > 1)
            {
                bitmap_map[final_keys[index]] = it->second;
            }
            else
            {
                auto it_final_item = bitmap_map.extract(it->first);
                it_final_item.key() = final_keys[index];
                bitmap_map.insert(std::move(it_final_item));
            }
            return;
        }

        std::map<UInt32, std::vector<roaring::Roaring*>> roaring_map;
        for (const auto & key: key_set)
        {
            auto it = bitmap_map.find(key);
            if (it == bitmap_map.end())
                continue;
            std::map<UInt32, roaring::Roaring> & inner_roaring = const_cast<std::map<UInt32, roaring::Roaring> &>(it->second.getRoarings());
            for (auto & jt : inner_roaring)
            {
                if (roaring_map.find(jt.first) == roaring_map.end())
                    roaring_map.emplace(jt.first, std::vector<roaring::Roaring *>());
                roaring_map[jt.first].emplace_back(&jt.second);
            }
        }

        BitMap64 res_roaring;

        for (auto & it : roaring_map)
        {
            roaring::Roaring result = roaring::Roaring::fastunion(it.second.size(), &(*(it.second.begin())));
            const_cast<std::map<UInt32, roaring::Roaring> &>(res_roaring.getRoarings()).emplace(it.first, std::move(result));
        }

        bitmap_map[final_keys[index]] = std::move(res_roaring);
    }

    void executeExpression(const AggregateFunctionBitMapData<String> & data, size_t index) const
    {
        if (expression_only_ors[index])
        {
            executeExpressionOnlyOr(data, index);
        }
        else
        {
            for (const auto & action : expression_actions_vector[index])
            {
                executeExpressionImpl(action.left, action.op, action.right, action.res, action.replicated, data);
            }
        }
    }
};

}
