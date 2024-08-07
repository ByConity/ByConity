#include <Optimizer/Property/Constants.h>

namespace DB
{

Constants Constants::translate(const std::unordered_map<String, String> & identities) const
{
    std::map<String, FieldWithType> translate_value;
    for (const auto & item : values)
        if (identities.contains(item.first))
            translate_value[identities.at(item.first)] = item.second;
        else
            translate_value[item.first] = item.second;
    return Constants{translate_value};
}

Constants Constants::normalize(const SymbolEquivalences & symbol_equivalences) const
{
    auto mapping = symbol_equivalences.representMap();
    for (const auto & item : values)
    {
        if (!mapping.contains(item.first))
        {
            mapping[item.first] = item.first;
        }
    }
    return translate(mapping);
}

String Constants::toString() const
{
    std::stringstream output;
    output << "{";
    for (const auto & item : values)
        output << " " << item.first << "=" << item.second.value.toString();
    output << "}";
    return output.str();
}

}
