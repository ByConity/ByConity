#include <DataTypes/Serializations/JSONBValue.h>
#include <Common/JSONParsers/jsonb/JSONBUtils.h>

namespace DB
{
bool JsonBinaryValue::fromJsonString(const char * s, int length)
{
    JsonbErrType error = JsonbErrType::E_NONE;
    if (!parser.parse(s, length))
    {
        error = parser.getErrorCode();
        auto msg = fmt::format("json parse error: {} for value: {}", JsonbErrMsg::getErrMsg(error), std::string_view(s, length));
        LOG_DEBUG(logger, msg);
        return false;
    }

    ptr = parser.getWriter().getOutput()->getBuffer();
    len = static_cast<unsigned>(parser.getWriter().getOutput()->getSize());
    return true;
}

std::string JsonBinaryValue::toJsonString() const
{
    return JsonbToJson::jsonb_to_json_string(ptr, len);
}

std::ostream & operator<<(std::ostream & os, const JsonBinaryValue & json_value)
{
    return os << json_value.toJsonString();
}
}
