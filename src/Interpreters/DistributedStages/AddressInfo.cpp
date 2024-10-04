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

#include <string>
#include <IO/ReadHelpers.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Protos/plan_node_utils.pb.h>
#include <boost/algorithm/string/join.hpp>


namespace DB
{
AddressInfo::AddressInfo(const String &host_name_, UInt16 port_, const String &user_, const String &password_)
        : host_name(host_name_), port(port_), user(user_), password(password_) {}

AddressInfo::AddressInfo(const String &host_name_, UInt16 port_, const String &user_, const String &password_, UInt16 exchange_port_)
        : host_name(host_name_), port(port_), user(user_), password(password_), exchange_port(exchange_port_) {}

AddressInfo::AddressInfo(const Protos::AddressInfo & proto) : host_name(proto.host_name()), port(proto.port()), exchange_port(proto.exchange_port())
{
    if (proto.has_user())
        user = proto.user();
    if (proto.has_password())
        password = proto.password();
}

void AddressInfo::serialize(WriteBuffer &buf) const
{
    // TODO: remove this when PlanSegment Protobuf is ready
    writeBinary(host_name, buf);
    writeBinary(port, buf);
    writeBinary(user, buf);
    writeBinary(password, buf);
    writeBinary(exchange_port, buf);
}

void AddressInfo::deserialize(ReadBuffer &buf)
{
    // TODO: remove this when PlanSegment Protobuf is ready
    readBinary(host_name, buf);
    readBinary(port, buf);
    readBinary(user, buf);
    readBinary(password, buf);
    readBinary(exchange_port, buf);
}

void AddressInfo::toProto(Protos::AddressInfo & proto) const
{
    proto.set_host_name(host_name);
    proto.set_port(port);
    proto.set_user(user);
    proto.set_password(password);
    proto.set_exchange_port(exchange_port);
}

void AddressInfo::fillFromProto(const Protos::AddressInfo & proto)
{
    host_name = proto.host_name();
    port = proto.port();
    user = proto.user();
    password = proto.password();
    exchange_port = proto.exchange_port();
}

String AddressInfo::toString() const
{
    return fmt::format("host_name: {}, port: {}, exchange_port: {} user: {}", host_name, port, exchange_port, user);
}

String AddressInfo::toShortString() const
{
    return fmt::format("{}:{}/{}", host_name, port, exchange_port);
}

void PlanSegmentMultiPartitionSource::toProto(Protos::PlanSegmentMultiPartitionSource & proto) const
{
    proto.set_exchange_id(exchange_id);
    address->toProto(*proto.mutable_address());
    for (auto p_id : partition_ids)
        proto.add_partition_ids(p_id);
}

void PlanSegmentMultiPartitionSource::fillFromProto(const Protos::PlanSegmentMultiPartitionSource & proto)
{
    exchange_id = proto.exchange_id();
    address = std::make_shared<AddressInfo>();
    address->fillFromProto(proto.address());
    partition_ids.reserve(proto.partition_ids().size());
    for (auto p_id : proto.partition_ids())
    {
        partition_ids.emplace_back(p_id);
    }
}

String PlanSegmentMultiPartitionSource::toString() const
{
    return fmt::format(
        "source[{} - partition_ids:{} - exchange_id:{}]",
        address->toShortString(),
        boost::algorithm::join(partition_ids | boost::adaptors::transformed([](UInt32 id) { return std::to_string(id); }), ","),
        exchange_id);
}
}


