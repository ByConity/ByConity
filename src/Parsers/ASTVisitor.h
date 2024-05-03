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

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFieldReference.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTPreparedParameter.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTQuantifiedComparison.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/IAST.h>
#include <QueryPlan/Void.h>
#include <common/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
}

template <typename R, typename C>
class ASTVisitor
{
public:
    constexpr static UInt64 MAX_RECURSION_LEVEL = 1024;

    explicit ASTVisitor(UInt64 max_level_ = MAX_RECURSION_LEVEL) : max_level(max_level_)
    {
    }
    virtual ~ASTVisitor() = default;
    virtual R visitNode(ASTPtr &, C &) { throw Exception("Visitor does not supported this AST node.", ErrorCodes::NOT_IMPLEMENTED); }
#define VISITOR_DEF(TYPE) \
    virtual R visit##TYPE(ASTPtr & node, C & context) { return visitNode(node, context); }
    APPLY_AST_TYPES(VISITOR_DEF)
#undef VISITOR_DEF

private:
    const UInt64 max_level;
    UInt64 level = 0;
    friend class ASTVisitorUtil;
};


template <typename R, typename C>
class ConstASTVisitor
{
public:
    constexpr static UInt64 MAX_RECURSION_LEVEL = 1024;

    explicit ConstASTVisitor(UInt64 max_level_ = MAX_RECURSION_LEVEL) : max_level(max_level_)
    {
    }
    virtual ~ConstASTVisitor() = default;
    virtual R visitNode(const ConstASTPtr &, C &) { throw Exception("Visitor does not supported this AST node.", ErrorCodes::NOT_IMPLEMENTED); }
#define VISITOR_DEF(TYPE) \
    virtual R visit##TYPE(const ConstASTPtr & node, C & context) { return visitNode(node, context); }
    APPLY_AST_TYPES(VISITOR_DEF)
#undef VISITOR_DEF

private:
    const UInt64 max_level;
    UInt64 level = 0;
    friend class ASTVisitorUtil;
};

class ASTVisitorUtil
{
public:
    template <typename R, typename C>
    static R accept(ASTPtr && node, ASTVisitor<R, C> & visitor, C & context)
    {
        return accept(node, visitor, context);
    }

    template <typename R, typename C>
    static R accept(ASTPtr & node, ASTVisitor<R, C> & visitor, C & context)
    {
        if (++visitor.level > visitor.max_level)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Too deep recursion");
        SCOPE_EXIT({ --visitor.level; });
#define VISITOR_DEF(TYPE) \
       if (node->getType() == ASTType::TYPE) \
       { \
           return visitor.visit##TYPE(node, context); \
       }
       APPLY_AST_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
       return visitor.visitNode(node, context);
    }

    template <typename R, typename C>
    static R accept(const ConstASTPtr & node, ConstASTVisitor<R, C> & visitor, C & context)
    {
        if (++visitor.level > visitor.max_level)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Too deep recursion");
        SCOPE_EXIT({ --visitor.level; });
#define VISITOR_DEF(TYPE) \
       if (node->getType() == ASTType::TYPE) \
       { \
           return visitor.visit##TYPE(node, context); \
       }
        APPLY_AST_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
        return visitor.visitNode(node, context);
    }
};

}
