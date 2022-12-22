#pragma once

#include <Core/Types.h>
#include <Protos/optimizer_statistics.pb.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
namespace DB
{
using SerdeDataType = Protos::SerdeDataType;
SerdeDataType SerdeDataTypeFromString(const String & tag_string);
String SerdeDataTypeToString(SerdeDataType tag);
using PArray = Poco::JSON::Array;
using PObject = Poco::JSON::Object;
using PVar = Poco::Dynamic::Var;
using Pparser = Poco::JSON::Parser;

class Json2Pb
{
public:
    typedef ::google::protobuf::Message ProtobufMsg;
    typedef ::google::protobuf::Reflection ProtobufReflection;
    typedef ::google::protobuf::FieldDescriptor ProtobufFieldDescriptor;
    typedef ::google::protobuf::Descriptor ProtobufDescriptor;

public:
    static void pbMsg2JsonStr(const ProtobufMsg &, String &, bool enum_str = false);
    static bool jsonStr2PbMsg(const String &, ProtobufMsg &, bool str_enum = false);
    static bool json2PbMsg(const Poco::JSON::Object &, ProtobufMsg &, bool);
    static void pbMsg2Json(const ProtobufMsg &, Poco::JSON::Object &, bool);

private:
    static bool json2RepeatedMessage(
        const Poco::JSON::Array &,
        ProtobufMsg &,
        const ProtobufFieldDescriptor * field,
        const ProtobufReflection * reflection,
        bool str_enum);
    static void repeatedMessage2Json(
        const ProtobufMsg & message,
        const ProtobufFieldDescriptor * field,
        const ProtobufReflection * reflection,
        Poco::JSON::Array & json,
        bool enum_str);
};
}
