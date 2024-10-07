#include <Storages/MergeTree/Index/BitmapIndexHelper.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexReader.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexBoolReader.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexSingleReader.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexMultipleReader.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexExpressionReader.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexIntersectBoolReader.h>
#include <Common/escapeForFileName.h>
#include <Common/Exception.h>
#include "Interpreters/ActionsVisitor.h"
#include "QueryPlan/IQueryPlanStep.h"
#include <Core/Names.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <set>
#include <map>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_IDENTIFIER;
extern const int UNEXPECTED_EXPRESSION;
extern const int TYPE_MISMATCH;
extern const int FUNCTION_NOT_ALLOWED;
}

static std::map<String, BitmapIndexReturnType> array_set_functions = {
    {"has", BitmapIndexReturnType::BOOL},
    {"hasAny", BitmapIndexReturnType::BOOL},
    {"hasAll", BitmapIndexReturnType::INTERSECT_BOOL},
    {"arraySetCheck", BitmapIndexReturnType::BOOL},
    {"arraySetGet", BitmapIndexReturnType::MULTIPLE},
    {"arraySetGetAny", BitmapIndexReturnType::SINGLE},
    {"bitmapHasAnyElement", BitmapIndexReturnType::BOOL}
};

static std::map<String, BitmapIndexReturnType> narrow_array_set_functions = {
    {"arraySetCheck", BitmapIndexReturnType::BOOL},
    {"arraySetGet", BitmapIndexReturnType::MULTIPLE},
    {"arraySetGetAny", BitmapIndexReturnType::SINGLE}
};

static std::map<String, BitmapIndexReturnType> bitmap_functions = {
    {"bitmapHasAnyElement", BitmapIndexReturnType::BOOL}
};

static std::unordered_set<String> func_inside_bitmap_function = {
    "assumeNotNull"
};

bool BitmapIndexHelper::hasNullArgument(const ASTPtr & ast)
{
    const auto* func = typeid_cast<const ASTFunction*>(ast.get());
    if (!func)
        return false;

    for (const auto& arg : func->arguments->children)
    {
        if (!arg)
            continue;

        if (const auto & literal = typeid_cast<const ASTLiteral*>(arg.get()))
        {
            if (literal->value.isNull())
            {
                return true;
            }
            else if (literal->value.isArray())
            {
                auto array = literal->value.get<const Array &>();
                if (std::any_of(array.begin(), array.end(), [](const auto & elem) { return elem.isNull(); }))
                    return true;
            }
            else if (literal->value.isTuple())
            {
                auto tuple = literal->value.get<const Tuple &>();
                if (std::any_of(tuple.begin(), tuple.end(), [](const auto & elem) { return elem.isNull(); }))
                    return true;
            }
        }

        if (hasNullArgument(arg))
            return true;
    }

    return false;
}


bool BitmapIndexHelper::checkConstArguments(const ASTPtr & ast, bool skip_even_columns) 
{
    if (!ast)
        return false;

    const auto * func = typeid_cast<const ASTFunction*>(ast.get());
    if (!func)
        return false;

    for (size_t i = 0; i < func->arguments->children.size(); ++i) 
    {
        ASTPtr argument = func->arguments->children.at(i);

        if (skip_even_columns && (i % 2 == 0))
            continue;

        if (const auto * literal = typeid_cast<const ASTLiteral*>(argument.get()))
            continue;

        if (const auto * function = typeid_cast<const ASTFunction*>(argument.get()))
        {
            if (checkConstArguments(argument, false))
                continue;
        }

        return false;
    }

    return true;
}

std::pair<NameSet, NameSet> BitmapIndexInfo::getIndexColumns(const IMergeTreeDataPartPtr & data_part)
{
    if (data_part->getType() != MergeTreeDataPartType::Value::CNCH || return_types.empty())
        return {};

    BitmapIndexReturnType return_type = BitmapIndexReturnType::UNKNOWN;
    for (const auto & it : return_types)
    {
        if (return_type == BitmapIndexReturnType::UNKNOWN)
            return_type = it.second;
        else
        {
            if (return_type != it.second)
                throw Exception("There are multiple return type when get bitmap index reader", ErrorCodes::LOGICAL_ERROR);
        }
    }

    if (return_type == BitmapIndexReturnType::UNKNOWN)
        return {};

    NameSet index_name_set;
    NameSet bitmap_columns;
    for (const auto & it : index_names)
    {
        bitmap_columns.insert(it.first);
        try
        {
            for (const auto & jt : it.second)
            {
                if (index_name_set.count(jt))
                    continue;

                String index_name = jt;
                index_name_set.insert(index_name);

                auto index_reader = std::make_shared<BitmapIndexReader>(data_part, index_name);
                if (index_reader && index_reader->valid())
                    continue;
                else
                    return {};
            }
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("MergeTreeBitmapIndexReader"), __PRETTY_FUNCTION__);
            return {};
        }
    }
    if (index_name_set.empty())
        return {};

    return std::make_pair(std::move(index_name_set), std::move(bitmap_columns));
}

bool BitmapIndexHelper::isArraySetFunctions(const String & name)
{
    return array_set_functions.count(name);
}

bool BitmapIndexHelper::isBitmapFunctions(const String & name)
{
    return bitmap_functions.count(name);
}

bool BitmapIndexHelper::isNarrowArraySetFunctions(const String & name)
{
    return narrow_array_set_functions.count(name);
}

bool BitmapIndexHelper::isValidBitMapFunctions(const ASTPtr & ast)
{
    if (!ast) return false;

    auto * func = ast->as<ASTFunction>();

    if (func)
    {
        if (isArraySetFunctions(func->name))
        {
            if (func->arguments)
            {
                for (size_t i = 0, arg_size = func->arguments->children.size(); i < arg_size; i += 2)
                {
                    ASTPtr arg = func->arguments->children.at(i);
                    if (auto * arg_func = arg->as<ASTFunction>(); arg_func && !func_inside_bitmap_function.count(arg_func->name))
                        return false;

                    if (!arg->as<ASTIdentifier>() && !arg->as<ASTLiteral>())
                        return false;
                }
                return true;
            }
        }
    } 

    return false;
}

BitmapIndexReturnType BitmapIndexHelper::getBitmapIndexReturnType(const String & name)
{
    auto it = array_set_functions.find(name);
    if (it != array_set_functions.end())
        return it->second;
    auto bitmap_it = bitmap_functions.find(name);
    if (bitmap_it != bitmap_functions.end())
        return bitmap_it->second;
    throw Exception("Cannot find function " + name + " to apply bitmap index", ErrorCodes::FUNCTION_NOT_ALLOWED);
}

void BitmapIndexInfo::buildIndexInfo(
    const ASTPtr & node,
    MergeTreeIndexInfo::BuildIndexContext & building_context,
    const StorageMetadataPtr & metadata_snapshot
)
{
    if (!node)
        return;

    if (auto * function = node->as<ASTFunction>())
    {
        bool is_array_set_func = BitmapIndexHelper::isValidBitMapFunctions(node);
        if (is_array_set_func)
        {
            bool should_update_bitmap_index_info = !index_names.count(function->getColumnName());
            size_t arg_size = function->arguments->children.size();
            if (arg_size % 2 != 0)
                throw Exception("The number of arguments is wrong in arraySet function", ErrorCodes::LOGICAL_ERROR);
        
            bool make_set = true;
            
            if (make_set)
            {
                // check if current column type really has bitmap index
                bool has_valid_identifier = false;
                for (size_t i = 0; i < arg_size; i += 2)
                {
                    ASTPtr arg_col = function->arguments->children.at(i);
                    if (auto * identifier = arg_col->as<ASTIdentifier>())
                    {
                        const auto & index_column_name = identifier->getColumnName();
                        if (metadata_snapshot && metadata_snapshot->getColumns().has(index_column_name))
                        {
                            if (!metadata_snapshot->getColumns().get(index_column_name).type->isBitmapIndex())
                            {
                                should_update_bitmap_index_info = false;
                                break;
                            }
                            has_valid_identifier = true;
                        }
                    }
                }
                should_update_bitmap_index_info &= has_valid_identifier;

                auto col_name = function->getColumnName();

                if (should_update_bitmap_index_info)
                    return_types.emplace(col_name, BitmapIndexHelper::getBitmapIndexReturnType(function->name));

                for (size_t i = 0; i < arg_size; i += 2)
                {
                    ASTPtr arg_col = function->arguments->children.at(i);
                    ASTPtr arg_set = function->arguments->children.at(i + 1);
                    // make set for argument
                    Names output;
                    output.emplace_back(arg_col->getColumnName());
                    auto action = IQueryPlanStep::createExpressionActions(building_context.context, building_context.input_columns, output, arg_col);
                    auto settings = building_context.context->getSettingsRef();
                    SizeLimits size_limits_for_set(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode);
                    SetPtr arg_prepared_set = makeExplicitSet(function, *action, true, building_context.context, size_limits_for_set, building_context.prepared_sets);
                    if (arg_prepared_set && should_update_bitmap_index_info)
                    {
                        if (auto * identifier = arg_col->as<ASTIdentifier>())
                        {
                            index_names[col_name].push_back(identifier->name());
                            set_args[col_name].push_back(arg_prepared_set);
                            // collect all col names
                            index_column_name_set.emplace(identifier->name());
                        }
                    }
                }
            }
        }
    }
    else
    {
        for (auto & child : node->children)
            buildIndexInfo(child, building_context, metadata_snapshot);
    }
}

void BitmapIndexInfo::initIndexes(const Names &)
{

}

void BitmapIndexInfo::setNonRemovableColumn(const String & column)
{
    if (index_column_name_set.count(column))
        non_removable_index_columns.insert(column);
}

const char * typeToString(BitmapIndexReturnType t)
{
    switch (t)
    {
        case BitmapIndexReturnType::BOOL:
            return "Bool";
        case BitmapIndexReturnType::INTERSECT_BOOL:
            return "Intersect Bool";
        case BitmapIndexReturnType::SINGLE:
            return "Single";
        case BitmapIndexReturnType::MULTIPLE:
            return "Multiple";
        default:
            return "Unknown";
    }
}

String BitmapIndexInfo::toString() const
{
    std::ostringstream ostr;

    ostr << "Bitmap Index:\n";
    for (const auto & index_name : index_names)
    {
        ostr << "result_column: " << index_name.first << ":\n";
        ostr << fmt::format("source_columns : {} ", index_name.second) << ",\n";
        if (set_args.count(index_name.first))
            ostr << fmt::format("source_sets size: {}", set_args.at(index_name.first).size()) << ",\n";
        if (return_types.count(index_name.first))
            ostr << fmt::format("return_type: {}", typeToString(return_types.at(index_name.first))) << ",\n";
        ostr << "\n";
    }
    for (const auto & bitmap_expression : bitmap_expressions)
        ostr << fmt::format("bitmap_expression: {} - {}", bitmap_expression.first, bitmap_expression.second) << ",\n";

    ostr << fmt::format("index_columns: {}", index_column_name_set) << ",\n";
    ostr << fmt::format("non_removable_index_columns: {}", non_removable_index_columns) << ",\n";
    ostr << fmt::format("remove_on_header_column_name_set: {}", remove_on_header_column_name_set);

    return ostr.str();
}

std::unique_ptr<MergeTreeBitmapIndexReader> BitmapIndexHelper::getBitmapIndexReader
    (
        const IMergeTreeDataPartPtr & part,
        const BitmapIndexInfoPtr & bitmap_index_info,
        const MergeTreeIndexGranularity & index_granularity, 
        const size_t & segment_granularity, 
        const size_t & serializing_granularity, 
        const MarkRanges & mark_ranges
    )
{
    if (!bitmap_index_info || bitmap_index_info->return_types.empty())
        return nullptr;
    BitmapIndexReturnType return_type = BitmapIndexReturnType::UNKNOWN;

    for (auto & it : bitmap_index_info->return_types)
    {
        if (return_type == BitmapIndexReturnType::UNKNOWN)
            return_type = it.second;
        else
        {
            if (return_type != it.second)
                throw Exception("There are multiple return type when get bitmap index reader", ErrorCodes::LOGICAL_ERROR);
        }
    }

    if (return_type == BitmapIndexReturnType::BOOL)
        return std::make_unique<MergeTreeBitmapIndexBoolReader>(part, bitmap_index_info, index_granularity, segment_granularity, serializing_granularity, mark_ranges);
    else if (return_type == BitmapIndexReturnType::INTERSECT_BOOL)
        return std::make_unique<MergeTreeBitmapIndexIntersectBoolReader>(part, bitmap_index_info, index_granularity, segment_granularity, serializing_granularity, mark_ranges);
    else if (return_type == BitmapIndexReturnType::EXPRESSION)
        return std::make_unique<MergeTreeBitmapIndexExpressionReader>(part, bitmap_index_info, index_granularity, segment_granularity, serializing_granularity, mark_ranges);
    else if (return_type == BitmapIndexReturnType::SINGLE)
        return std::make_unique<MergeTreeBitmapIndexSingleReader>(part, bitmap_index_info, index_granularity, segment_granularity, serializing_granularity, mark_ranges);
    else if (return_type == BitmapIndexReturnType::MULTIPLE)
        return std::make_unique<MergeTreeBitmapIndexMultipleReader>(part, bitmap_index_info, index_granularity, segment_granularity, serializing_granularity, mark_ranges);
    else
        throw Exception("Cannot get a bitmap index reader since the return type is UNKNOWN", ErrorCodes::LOGICAL_ERROR);
}

std::unique_ptr<MergeTreeBitmapIndexReader> BitmapIndexHelper::getBitmapIndexReader
    (
        const IMergeTreeDataPartPtr & part,
        const BitmapIndexInfoPtr & bitmap_index_info,
        const MergeTreeIndexGranularity & index_granularity, 
        const MarkRanges & mark_ranges
    )
{
    return getBitmapIndexReader(part, bitmap_index_info, index_granularity, part->storage.getSettings()->bitmap_index_segment_granularity, part->storage.getSettings()->bitmap_index_serializing_granularity, mark_ranges);
}


String BitmapIndexInfo::dump() const
{
    std::stringstream ss;
    ss << "  Set arguments: ";
    for (const auto & [name, v] : set_args)
        ss << "<" << name << ", " << v.size() << "-elements vector> ";
    ss << "\n";
    ss << "  Index names: ";
    for (const auto & [name, v] : index_names)
        ss << "<" << name << ", " << v.size() << "-elements vector> ";
    ss << "\n";
    ss << "  Return type: ";
    for (const auto & [name, v] : return_types)
        ss << "<" << name << ", " << typeToString(v) << "> ";
    ss << "\n";
    ss << "  Index name set: ";
    for (const auto & name : index_column_name_set)
        ss << name << " ";
    ss << "\n";
    ss << "  Non removable index columns: ";
    for (const auto & name : non_removable_index_columns)
        ss << name << " ";
    ss << "\n";
    return ss.str();
}

void BitmapIndexInfo::updateHeader(Block & header, bool remove_indexed_columns) const
{
    for (const auto & [name, type] : return_types)
    {
        if (header.has(name)) 
            continue;
        if (type == BitmapIndexReturnType::EXPRESSION)
            header.insertUnique({DataTypeUInt8().createColumn(), std::make_shared<DataTypeUInt8>(), name});
        if (!set_args.contains(name))
            continue; // may be throw?
        const auto & args = set_args.at(name);
        const auto data_type = args[0]->getDataTypes().at(0);
        auto array_type = std::make_shared<DataTypeArray>(data_type);
        auto nullable_type = std::make_shared<DataTypeNullable>(data_type);

        switch (type)
        {
            case BitmapIndexReturnType::BOOL:
                header.insertUnique({DataTypeUInt8().createColumn(), std::make_shared<DataTypeUInt8>(), name});
                break;
            case BitmapIndexReturnType::INTERSECT_BOOL:
                header.insertUnique({DataTypeUInt8().createColumn(), std::make_shared<DataTypeUInt8>(), name});
                break;
            case BitmapIndexReturnType::SINGLE:
                header.insertUnique({nullable_type->createColumn(), nullable_type, name});
                break;
            case BitmapIndexReturnType::MULTIPLE:
                header.insertUnique({array_type->createColumn(), array_type, name});
                break;
            case BitmapIndexReturnType::EXPRESSION:
            case BitmapIndexReturnType::UNKNOWN:
                break;
        }
    }
    if (remove_indexed_columns) /// TODO @canh: use non_removable_index_columns later, not work yet
    {
        for (const auto & name : index_column_name_set)
        {
            if (header.has(name))
                header.erase(name);
        }
    }
}

Block BitmapIndexInfo::updateHeader(Block header)
{
    for (const auto & name : header.getNames())
    {
        if (remove_on_header_column_name_set.count(name) > 0 && non_removable_index_columns.count(name) <= 0)
        {
            header.erase(name);
        }
    }
    for (const auto & [name, type] : return_types)
    {
        if (header.has(name))
            continue;
        if (type == BitmapIndexReturnType::EXPRESSION)
            header.insertUnique({DataTypeUInt8().createColumn(), std::make_shared<DataTypeUInt8>(), name});
    }
    Block res;
    for (const auto & item : header)
        res.insert(std::move(item));
    return res;
}

bool functionCanUseBitmapIndex(const ASTFunction & function)
{
    return array_set_functions.count(function.name) || bitmap_functions.count(function.name);
}
}
