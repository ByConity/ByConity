#include <Parsers/IAST.h>
#include <Parsers/ASTSystemQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


const char * ASTSystemQuery::typeToString(Type type)
{
    switch (type)
    {
        case Type::SHUTDOWN:
            return "SHUTDOWN";
        case Type::KILL:
            return "KILL";
        case Type::SUSPEND:
            return "SUSPEND";
        case Type::DROP_DNS_CACHE:
            return "DROP DNS CACHE";
        case Type::DROP_MARK_CACHE:
            return "DROP MARK CACHE";
        case Type::DROP_UNCOMPRESSED_CACHE:
            return "DROP UNCOMPRESSED CACHE";
        case Type::DROP_MMAP_CACHE:
            return "DROP MMAP CACHE";
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
            return "DROP COMPILED EXPRESSION CACHE";
#endif
        case Type::STOP_LISTEN_QUERIES:
            return "STOP LISTEN QUERIES";
        case Type::START_LISTEN_QUERIES:
            return "START LISTEN QUERIES";
        case Type::RESTART_REPLICAS:
            return "RESTART REPLICAS";
        case Type::RESTART_REPLICA:
            return "RESTART REPLICA";
        case Type::RESTORE_REPLICA:
            return "RESTORE REPLICA";
        case Type::DROP_REPLICA:
            return "DROP REPLICA";
        case Type::SYNC_REPLICA:
            return "SYNC REPLICA";
        case Type::SYNC_MUTATION:
            return "SYNC MUTATION";
        case Type::EXECUTE_MUTATION:
            return "EXECUTE MUTATION";
        case Type::RELOAD_MUTATION:
            return "RELOAD MUTATION";
        case Type::FLUSH_DISTRIBUTED:
            return "FLUSH DISTRIBUTED";
        case Type::RELOAD_DICTIONARY:
            return "RELOAD DICTIONARY";
        case Type::RELOAD_DICTIONARIES:
            return "RELOAD DICTIONARIES";
        case Type::RELOAD_MODEL:
            return "RELOAD MODEL";
        case Type::RELOAD_MODELS:
            return "RELOAD MODELS";
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
            return "RELOAD EMBEDDED DICTIONARIES";
        case Type::RELOAD_CONFIG:
            return "RELOAD CONFIG";
        case Type::RELOAD_FORMAT_SCHEMA:
            return "RELOAD FORMAT SCHEMA";
        case Type::RELOAD_SYMBOLS:
            return "RELOAD SYMBOLS";
        case Type::STOP_MERGES:
            return "STOP MERGES";
        case Type::START_MERGES:
            return "START MERGES";
        case Type::STOP_TTL_MERGES:
            return "STOP TTL MERGES";
        case Type::START_TTL_MERGES:
            return "START TTL MERGES";
        case Type::STOP_MOVES:
            return "STOP MOVES";
        case Type::START_MOVES:
            return "START MOVES";
        case Type::STOP_FETCHES:
            return "STOP FETCHES";
        case Type::START_FETCHES:
            return "START FETCHES";
        case Type::STOP_REPLICATED_SENDS:
            return "STOP REPLICATED SENDS";
        case Type::START_REPLICATED_SENDS:
            return "START REPLICATED SENDS";
        case Type::STOP_REPLICATION_QUEUES:
            return "STOP REPLICATION QUEUES";
        case Type::START_REPLICATION_QUEUES:
            return "START REPLICATION QUEUES";
        case Type::STOP_DISTRIBUTED_SENDS:
            return "STOP DISTRIBUTED SENDS";
        case Type::START_DISTRIBUTED_SENDS:
            return "START DISTRIBUTED SENDS";
        case Type::FLUSH_LOGS:
            return "FLUSH LOGS";
        case Type::RESTART_DISK:
            return "RESTART DISK";
        case Type::SKIP_LOG:
            return "SKIP LOG";
        case Type::EXECUTE_LOG:
            return "EXECUTE LOG";
        case Type::SET_VALUE:
            return "SET VALUE";
        case Type::MARK_LOST:
            return "MARK LOST";
        case Type::START_CONSUME:
            return "START CONSUME";
        case Type::STOP_CONSUME:
            return "STOP CONSUME";
        case Type::RESTART_CONSUME:
            return "RESTART CONSUME";
        case Type::FETCH_PARTS:
            return "FETCH PARTS INTO";
        default:
            throw Exception("Unknown SYSTEM query command", ErrorCodes::LOGICAL_ERROR);
    }
}


void ASTSystemQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SYSTEM ";
    settings.ostr << typeToString(type) << (settings.hilite ? hilite_none : "");

    auto print_database_table = [&]
    {
        settings.ostr << " ";
        if (!database.empty())
        {
            settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(database)
                          << (settings.hilite ? hilite_none : "") << ".";
        }
        settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(table)
                      << (settings.hilite ? hilite_none : "");
    };

    auto print_drop_replica = [&]
    {
        settings.ostr << " " << quoteString(replica);
        if (!table.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM TABLE"
                          << (settings.hilite ? hilite_none : "");
            print_database_table();
        }
        else if (!replica_zk_path.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM ZKPATH "
                          << (settings.hilite ? hilite_none : "") << quoteString(replica_zk_path);
        }
        else if (!database.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM DATABASE "
                          << (settings.hilite ? hilite_none : "");
            settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(database)
                          << (settings.hilite ? hilite_none : "");
        }
    };

    auto print_on_volume = [&]
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON VOLUME "
                      << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(storage_policy)
                      << (settings.hilite ? hilite_none : "")
                      << "."
                      << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(volume)
                      << (settings.hilite ? hilite_none : "");
    };

    if (!cluster.empty())
        formatOnCluster(settings);

    if (   type == Type::STOP_MERGES
        || type == Type::START_MERGES
        || type == Type::STOP_TTL_MERGES
        || type == Type::START_TTL_MERGES
        || type == Type::STOP_MOVES
        || type == Type::START_MOVES
        || type == Type::STOP_FETCHES
        || type == Type::START_FETCHES
        || type == Type::STOP_REPLICATED_SENDS
        || type == Type::START_REPLICATED_SENDS
        || type == Type::STOP_REPLICATION_QUEUES
        || type == Type::START_REPLICATION_QUEUES
        || type == Type::STOP_DISTRIBUTED_SENDS
        || type == Type::START_DISTRIBUTED_SENDS)
    {
        if (!table.empty())
            print_database_table();
        else if (!volume.empty())
            print_on_volume();
    }
    else if (  type == Type::RESTART_REPLICA
            || type == Type::RESTORE_REPLICA
            || type == Type::SYNC_REPLICA
            || type == Type::SYNC_MUTATION
            || type == Type::MARK_LOST
            || type == Type::FLUSH_DISTRIBUTED
            || type == Type::RELOAD_DICTIONARY
             || type == Type::START_CONSUME
             || type == Type::STOP_CONSUME
             || type == Type::RESTART_CONSUME)
    {
        print_database_table();
    }
    else if (   type == Type::EXECUTE_MUTATION
             || type == Type::RELOAD_MUTATION)
    {
        WriteBufferFromOwnString wb;
        writeQuoted(mutation_id, wb);
        settings.ostr << " " << wb.str()
                      << (settings.hilite ? hilite_keyword : "") << " ON"
                      << (settings.hilite ? hilite_none : "");
        print_database_table();
    }
    else if (type == Type::DROP_REPLICA)
    {
        print_drop_replica();
    }
    else if (type == Type::SUSPEND)
    {
         settings.ostr << (settings.hilite ? hilite_keyword : "") << " FOR "
            << (settings.hilite ? hilite_none : "") << seconds
            << (settings.hilite ? hilite_keyword : "") << " SECOND"
            << (settings.hilite ? hilite_none : "");
    }
    else if (type == Type::SKIP_LOG || type == Type::EXECUTE_LOG)
    {
        print_database_table();
        settings.ostr << " ";
        predicate->formatImpl(settings, state, frame);
    }
    else if (type == Type::SET_VALUE)
    {
        print_database_table();
        settings.ostr << " ";
        values_changes->formatImpl(settings, state, frame);
    }
    else if (type == Type::FETCH_PARTS)
    {
        print_database_table();
        settings.ostr << " ";
        target_path->formatImpl(settings, state, frame);
    }
}

ASTPtr ASTSystemQuery::clone() const
{
    auto res = std::make_shared<ASTSystemQuery>(*this);
    res->children.clear();

    if (predicate)
    {
        res->predicate = predicate->clone();
        res->children.push_back(res->predicate);
    }
    if (values_changes)
    {
        res->values_changes = values_changes->clone();
        res->children.push_back(res->values_changes);
    }

    return res;
}


}
