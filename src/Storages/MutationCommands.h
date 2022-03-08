#pragma once

#include <Parsers/ASTAlterQuery.h>
#include <Storages/IStorage_fwd.h>
#include <DataTypes/IDataType.h>
#include <Core/Names.h>

#include <optional>
#include <unordered_map>


namespace DB
{

class Context;
class WriteBuffer;
class ReadBuffer;

/// Represents set of actions which should be applied
/// to values from set of columns which satisfy predicate.
struct MutationCommand
{
    ASTPtr ast; /// The AST of the whole command

    enum Type
    {
        EMPTY,     /// Not used.
        DELETE,
        FAST_DELETE,
        UPDATE,
        MATERIALIZE_INDEX,
        MATERIALIZE_PROJECTION,
        ADD_COLUMN, /// For detecting conflicts
        READ_COLUMN, /// Read column and apply conversions (MODIFY COLUMN alter query).
        DROP_COLUMN,
        DROP_INDEX,
        DROP_PROJECTION,
        MATERIALIZE_TTL,
        RENAME_COLUMN,
        CLEAR_MAP_KEY,
    };

    Type type = EMPTY;

    /// WHERE part of mutation
    ASTPtr predicate;

    /// Columns with corresponding actions
    std::unordered_map<String, ASTPtr> column_to_update_expression;

    /// For MATERIALIZE INDEX and PROJECTION
    String index_name;
    String projection_name;

    /// For MATERIALIZE INDEX, UPDATE and DELETE/FASTDELETE.
    ASTPtr partition;

    /// For CLEAR MAP KEYS
    ASTPtr map_keys;

    /// For reads, drops and etc.
    String column_name;
    DataTypePtr data_type; /// Maybe empty if we just want to drop column

    /// We need just clear column, not drop from metadata.
    bool clear = false;

    /// Column rename_to
    String rename_to;

    /// For FASTDELETE columns
    ASTPtr columns;

    /// If parse_alter_commands, than consider more Alter commands as mutation commands
    static std::optional<MutationCommand> parse(ASTAlterCommand * command, bool parse_alter_commands = false);
};

/// Multiple mutation commands, possible from different ALTER queries
class MutationCommands : public std::vector<MutationCommand>
{
public:
    std::shared_ptr<ASTExpressionList> ast() const;

    bool willMutateData() const;

    /// whether current commands can be executed together with other commands
    bool requireIndependentExecution() const;

    bool allOf(MutationCommand::Type type) const;

    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
};

}
