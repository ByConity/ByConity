#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>


namespace DB
{

namespace Nested
{
    std::string concatenateName(const std::string & nested_table_name, const std::string & nested_field_name);

    std::pair<std::string, std::string> splitName(const std::string & name);

    /// Returns the prefix of the name to the first '.'. Or the name is unchanged if there is no dot.
    std::string extractTableName(const std::string & nested_name);

    /// Replace Array(Tuple(...)) columns to a multiple of Array columns in a form of `column_name.element_name`.
    /// only for named tuples that actually represent Nested structures.
    Block flatten(const Block & block);

    /// Collect Array columns in a form of `column_name.element_name` to single Array(Tuple(...)) column.
    NamesAndTypesList collect(const NamesAndTypesList & names_and_types);

    /// Convert old-style nested (single arrays with same prefix, `n.a`, `n.b`...) to subcolumns of data type Nested.
    NamesAndTypesList convertToSubcolumns(const NamesAndTypesList & names_and_types);

    /// Check that sizes of arrays - elements of nested data structures - are equal.
    void validateArraySizes(const Block & block);

    /// Extract all column names that are nested for specifying table.
    Names getAllNestedColumnsForTable(const Block & block, const std::string & table_name);
    
    /// Get all nested tables names from a block.
    std::unordered_set<String> getAllTableNames(const Block & block, bool to_lower_case = false);

}

/// Use this class to extract element columns from columns of nested type in a block, e.g. named Tuple.
/// It can extract a column from a multiple nested type column, e.g. named Tuple in named Tuple
/// Keeps some intermediate data to avoid rebuild them multi-times.
class NestedColumnExtractHelper
{
public:
    explicit NestedColumnExtractHelper(const Block & block_, bool case_insentive_);
    std::optional<ColumnWithTypeAndName> extractColumn(const String & column_name);
private:
    std::optional<ColumnWithTypeAndName>
    extractColumn(const String & original_column_name, const String & column_name_prefix, const String & column_name_suffix);
    const Block & block;
    bool case_insentive;
    std::map<String, BlockPtr> nested_tables;
};

}
