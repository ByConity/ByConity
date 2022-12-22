#include <Storages/Kafka/KafkaSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(KafkaSettingsTraits, LIST_OF_KAFKA_SETTINGS)

void KafkaSettings::applyKafkaSettingChanges(const SettingsChanges & changes)
{
    for (const auto & setting : changes)
    {
#define SET(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
else if (setting.name == "kafka_" #NAME) set(#NAME, setting.value); \
else if (setting.name == #NAME) set(#NAME, setting.value);

        if (false)
        {
        }
        KAFKA_RELATED_SETTINGS(SET)
        else throw Exception("Unknown Kafka setting " + setting.name, ErrorCodes::BAD_ARGUMENTS);
#undef SET
    }

}

void KafkaSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            applyKafkaSettingChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

void sortKafkaSettings(IAST & settings_ast)
{
    std::unordered_map<String, size_t> seq_map;

    size_t seq_no = 0;
#define SET(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
seq_map[#NAME] = ++seq_no; \
seq_map["kafka_" #NAME] = ++seq_no;

    KAFKA_RELATED_SETTINGS(SET)
#undef SET

auto get_seq_no = [&](const String & name) {
        if (auto it = seq_map.find(name); it != seq_map.end())
            return it->second;
        else
            return size_t(0);
    };

    auto & changes = settings_ast.as<ASTSetQuery &>().changes;

    std::sort(changes.begin(), changes.end(), [&](auto & lhs, auto & rhs) {
        return get_seq_no(lhs.name) < get_seq_no(rhs.name);
    });
}



}
