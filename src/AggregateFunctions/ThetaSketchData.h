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

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif


#include <memory>
#include <Statistics/DataSketchesHelper.h>
#include <boost/noncopyable.hpp>


namespace DB
{


template <typename Key>
class ThetaSketchData : private boost::noncopyable
{
private:
    std::unique_ptr<datasketches::update_theta_sketch> sk_update;
    std::unique_ptr<datasketches::theta_union> sk_union;

    inline datasketches::update_theta_sketch * getSkUpdate()
    {
        if (!sk_update)
            sk_update = std::make_unique<datasketches::update_theta_sketch>(datasketches::update_theta_sketch::builder().build());
        return sk_update.get();
    }

    inline datasketches::theta_union * getSkUnion()
    {
        if (!sk_union)
            sk_union = std::make_unique<datasketches::theta_union>(datasketches::theta_union::builder().build());
        return sk_union.get();
    }

public:
    using value_type = Key;

    ThetaSketchData() = default;
    ~ThetaSketchData() = default;

    /// Insert original value without hash, as `datasketches::update_theta_sketch.update` will do the hash internal.
    void insertOriginal(StringRef value)
    {
        getSkUpdate()->update(value.data, value.size);
    }

    /// Note that `datasketches::update_theta_sketch.update` will do the hash again.
    void insert(Key value)
    {
        getSkUpdate()->update(value);
    }

    UInt64 size() const
    {
        auto u = sk_union ? *sk_union : datasketches::theta_union::builder().build();
        if (sk_update)
        {
            u.update(*sk_update);
        }
        return u.get_result().get_estimate();
    }

    void merge(const ThetaSketchData & rhs)
    {
        datasketches::theta_union * u = getSkUnion();

        if (sk_update)
        {
            u->update(*sk_update);
            sk_update.reset(nullptr);
        }

        if (rhs.sk_update)
            u->update(*rhs.sk_update);
        if (rhs.sk_union)
            u->update(rhs.sk_union->get_result());
    }

    void intersect(const ThetaSketchData & rhs)
    {
        datasketches::theta_union * u = getSkUnion();

        if (sk_update)
        {
            u->update(*sk_update);
            sk_update.reset(nullptr);
        }

        datasketches::theta_intersection theta_intersection;

        theta_intersection.update(u->get_result());

        if (rhs.sk_update)
            theta_intersection.update(*rhs.sk_update);
        else if (rhs.sk_union)
            theta_intersection.update(rhs.sk_union->get_result());

        sk_union.reset(nullptr);
        u = getSkUnion();
        u->update(theta_intersection.get_result());
    }

    void aNotB(const ThetaSketchData & rhs)
    {
        datasketches::theta_union * u = getSkUnion();

        if (sk_update)
        {
            u->update(*sk_update);
            sk_update.reset(nullptr);
        }

        datasketches::theta_a_not_b a_not_b;

        if (rhs.sk_update)
        {
            datasketches::compact_theta_sketch result = a_not_b.compute(u->get_result(), *rhs.sk_update);
            sk_union.reset(nullptr);
            u = getSkUnion();
            u->update(result);
        }
        else if (rhs.sk_union)
        {
            datasketches::compact_theta_sketch result = a_not_b.compute(u->get_result(), rhs.sk_union->get_result());
            sk_union.reset(nullptr);
            u = getSkUnion();
            u->update(result);
        }
    }

    /// You can only call for an empty object.
    void read(DB::ReadBuffer & in)
    {
        datasketches::compact_theta_sketch::vector_bytes bytes;
        readVectorBinary(bytes, in);
        if (!bytes.empty())
        {
            auto sk = datasketches::compact_theta_sketch::deserialize(bytes.data(), bytes.size());
            getSkUnion()->update(sk);
        }
    }

    void write(DB::WriteBuffer & out) const
    {
        if (sk_union)
        {
            auto u = *sk_union;
            if (sk_update)
            {
                u.update(*sk_update);
            }
            auto bytes = u.get_result().serialize();
            writeVectorBinary(bytes, out);
        }
        else if (sk_update)
        {
            auto bytes = sk_update->compact().serialize();
            writeVectorBinary(bytes, out);
        }
        else
        {
            datasketches::compact_theta_sketch::vector_bytes bytes;
            writeVectorBinary(bytes, out);
        }
    }
};


}
