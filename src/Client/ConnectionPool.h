/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <Common/Logger.h>
#include <Common/PoolBase.h>
#include <Client/Connection.h>
#include <IO/ConnectionTimeouts.h>
#include <Core/Settings.h>

namespace DB
{

/** Interface for connection pools.
  *
  * Usage (using the usual `ConnectionPool` example)
  * ConnectionPool pool(...);
  *
  *    void thread()
  *    {
  *        auto connection = pool.get();
  *        connection->sendQuery(...);
  *    }
  */

class IConnectionPool : private boost::noncopyable
{
public:
    using Entry = PoolBase<Connection>::Entry;

    virtual ~IConnectionPool() = default;

    /// Selects the connection to work.
    /// If force_connected is false, the client must manually ensure that returned connection is good.
    virtual Entry get(const ConnectionTimeouts & timeouts,
                      const Settings * settings = nullptr,
                      bool force_connected = true) = 0;

    virtual Int64 getPriority() const { return 1; }
};

using ConnectionPoolPtr = std::shared_ptr<IConnectionPool>;
using ConnectionPoolPtrs = std::vector<ConnectionPoolPtr>;

/** A common connection pool, without fault tolerance.
  */
class ConnectionPool : public IConnectionPool, private PoolBase<Connection>
{
public:
    using Entry = IConnectionPool::Entry;
    using Base = PoolBase<Connection>;

    ConnectionPool(unsigned max_connections_,
            const String & host_,
            UInt16 port_,
            const String & default_database_,
            const String & user_,
            const String & password_,
            const String & cluster_,
            const String & cluster_secret_,
            const String & client_name_,
            Protocol::Compression compression_,
            Protocol::Secure secure_,
            Int64 priority_ = 1,
            UInt16 exchange_port_ = 0,
            UInt16 exchange_status_port_ = 0,
            UInt16 rpc_port_ = 0,
            String worker_id_ = "virtual_id")
        : Base(max_connections_,
        getLogger("ConnectionPool (" + host_ + ":" + toString(port_) + ")")),
        host(host_),
        port(port_),
        default_database(default_database_),
        user(user_),
        password(password_),
        cluster(cluster_),
        cluster_secret(cluster_secret_),
        client_name(client_name_),
        compression(compression_),
        secure(secure_),
        priority(priority_),
        exchange_port(exchange_port_),
        exchange_status_port(exchange_status_port_),
        rpc_port(rpc_port_),
        worker_id(worker_id_)
    {
    }

    Entry get(const ConnectionTimeouts & timeouts,
              const Settings * settings = nullptr,
              bool force_connected = true) override
    {
        Entry entry;
        if (settings)
            entry = Base::get(settings->connection_pool_max_wait_ms.totalMilliseconds());
        else
            entry = Base::get(-1);

        if (force_connected)
            entry->forceConnected(timeouts);

        return entry;
    }

    const std::string & getHost() const
    {
        return host;
    }
    std::string getDescription() const
    {
        return host + ":" + toString(port);
    }

    Int64 getPriority() const override
    {
        return priority;
    }

protected:
    /** Creates a new object to put in the pool. */
    ConnectionPtr allocObject() override
    {
        return std::make_shared<Connection>(
            host, port,
            default_database, user, password,
            cluster, cluster_secret,
            client_name, compression, secure,
            Poco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0),
            exchange_port, exchange_status_port, rpc_port, worker_id);
    }

private:
    String host;
    UInt16 port;
    String default_database;
    String user;
    String password;

    /// For inter-server authorization
    String cluster;
    String cluster_secret;

    String client_name;
    Protocol::Compression compression; /// Whether to compress data when interacting with the server.
    Protocol::Secure secure;           /// Whether to encrypt data when interacting with the server.
    Int64 priority;                    /// priority from <remote_servers>
    UInt16 exchange_port;
    UInt16 exchange_status_port;
    UInt16 rpc_port;
    String worker_id;
};

}
