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

#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Optimizer/LiteralEncoder.h>
#include <Analyzers/TypeAnalyzer.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST.h>

#include <unordered_map>

namespace DB
{

/** ExpressionInterpreter evaluates an expression with following functionalities:
  *   1. Constant folding, i.e. constant expression will be substituted with its result.
  *      Note that the expression don't have to be completely constant. E.g. `a + (1 + 2)`
  *      will be evaluated to `a + 3`. Also note that not all functions are suitable for
  *      constant folding, counter examples: undeterministic functions, aggregate functions,
  *      IN expressions...;
  *   2. Null simplify. Functions calls with null constant argument can be substituted with
  *      null. See also PreparedFunctionImpl::useDefaultImplementationForNulls;
  *   3. Function simplify. Some functions can be simplify by its intrinsic logic, even if
  *      not all arguments are constants. E.g., `X OR 1` can be simplified to `1`;
  */
class ExpressionInterpreter
{
public:
    using IdentifierTypes = NameToType;
    using IdentifierValues = std::unordered_map<String, Field>;

    // `optimizeExpression` simplify an expr by constant folding.
    static std::pair<DataTypePtr, ASTPtr> optimizeExpression(const ConstASTPtr & expression, IdentifierTypes types, ContextMutablePtr context)
    {
        auto interpreter = basicInterpreter(std::move(types), std::move(context));
        return interpreter.optimizeExpression(expression);
    }

    // `evaluateConstantExpression` evaluate a constant expr by constant folding, if not successful, return a std::nullopt.
    static std::optional<std::pair<DataTypePtr, Field>> evaluateConstantExpression(const ConstASTPtr & expression, IdentifierTypes types, ContextMutablePtr context)
    {
        auto interpreter = basicInterpreter(std::move(types), std::move(context));
        return interpreter.evaluateConstantExpression(expression);
    }

    // `optimizePredicate` simplify an expr by constant folding, null simplify and function simplify.
    // The simplified expr may have a different type with the origin expr. e.g. given `x` is a Nullable(UInt8) column,
    // `x OR 1` is of type Nullable(UInt8), while the simplified expr `1` is of type UInt8. In some cases, that will
    // lead to a wrong answer, e.g. `toTypeName(x OR 1) = 'Nullable(UInt8)'`, but those should be rare.
    static ASTPtr optimizePredicate(const ConstASTPtr & expression, IdentifierTypes types, ContextMutablePtr context, IdentifierValues values = {})
    {
        auto interpreter = optimizedInterpreter(std::move(types), std::move(values), std::move(context));
        return interpreter.optimizePredicate(expression);
    }

    struct InterpretSetting
    {
        IdentifierTypes identifier_types;    // any identifiers may occur in the expression, with its type
        IdentifierValues identifier_values;  // any identifiers may occur in the expression, with its substituted value
        bool enable_null_simplify = false;
        bool enable_function_simplify = false;
    };

    struct InterpretResult
    {
        DataTypePtr type;

        // one of members is valid
        ASTPtr ast;
        Field value;

        InterpretResult() = default;
        InterpretResult(DataTypePtr type_, ASTPtr ast_) : type(std::move(type_)), ast(std::move(ast_))
        {
        }
        InterpretResult(DataTypePtr type_, Field value_) : type(std::move(type_)), value(std::move(value_))
        {
        }

        bool isAST() const
        {
            return ast != nullptr;
        }
        bool isValue() const
        {
            return ast == nullptr;
        }
        const Field & getField() const
        {
            assert(isValue());
            return value;
        }
        ASTPtr convertToAST(const ContextMutablePtr & ctx) const;
    };

    ExpressionInterpreter(InterpretSetting setting_, ContextMutablePtr context_);
    static ExpressionInterpreter basicInterpreter(IdentifierTypes types, ContextMutablePtr context);
    static ExpressionInterpreter optimizedInterpreter(IdentifierTypes types, IdentifierValues values, ContextMutablePtr context);

    std::pair<DataTypePtr, ASTPtr> optimizeExpression(const ConstASTPtr & expression) const;
    ASTPtr optimizePredicate(const ConstASTPtr & expression) const;
    std::optional<std::pair<DataTypePtr, Field>> evaluateConstantExpression(const ConstASTPtr & expression) const;
    InterpretResult evaluate(const ConstASTPtr & expression) const;

private:
    ContextMutablePtr context;
    InterpretSetting setting;
    TypeAnalyzer type_analyzer;

/// public for type alias
public:
    struct InterpretIMResult
    {
        DataTypePtr type;

        // value and ast can be both valid, the ast will be used if the value is not suitable for ASTLiteral
        ASTPtr ast;
        ColumnPtr value;

        InterpretIMResult() = default;
        InterpretIMResult(DataTypePtr type_, ASTPtr ast_) : type(std::move(type_)), ast(std::move(ast_))
        {
        }
        InterpretIMResult(DataTypePtr type_, ASTPtr ast_, ColumnPtr value_)
            : type(std::move(type_)), ast(std::move(ast_)), value(std::move(value_))
        {
        }
        InterpretIMResult(DataTypePtr type_, ASTPtr ast_, const Field & field);

        bool isAST() const
        {
            return value == nullptr;
        }
        bool isValue() const
        {
            return value != nullptr;
        }
        Field getField() const
        {
            assert(isValue());
            return (*value)[0];
        }
        bool isNull() const;
        bool isSuitablyRepresentedByValue() const;
        ASTPtr convertToAST(const ContextMutablePtr & ctx) const;
    };

    using InterpretIMResults = std::vector<InterpretIMResult>;

private:
    InterpretIMResult visit(const ConstASTPtr & node) const;
    InterpretIMResult visitASTLiteral(const ASTLiteral & literal, const ConstASTPtr & node) const;
    InterpretIMResult visitASTIdentifier(const ASTIdentifier & identifier, const ConstASTPtr & node) const;
    InterpretIMResult visitASTPreparedParameter(const ASTPreparedParameter & prepared_param, const ConstASTPtr & node) const;
    InterpretIMResult visitOrdinaryFunction(const ASTFunction & function, const ConstASTPtr & node) const;
    InterpretIMResult visitInFunction(const ASTFunction & function, const ConstASTPtr & node) const;

    DataTypePtr getType(const ConstASTPtr & node) const
    {
        return type_analyzer.getType(node);
    }

    InterpretIMResult originalNode(const ConstASTPtr & node) const
    {
        return {getType(node), node->clone()};
    }
};

}
