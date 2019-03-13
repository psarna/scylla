/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "bytes.hh"

class schema;
class partition_key;
class clustering_row;

class column_computation;
using column_computation_ptr = std::unique_ptr<column_computation>;

class column_computation {
public:
    virtual ~column_computation() = default;

    static column_computation_ptr deserialize(bytes_view raw);
    static column_computation_ptr deserialize(const Json::Value& json);

    virtual column_computation_ptr clone() const = 0;

    virtual bytes serialize() const = 0;
    virtual bytes_opt compute_value(const schema& schema, const partition_key& key, const clustering_row& row) const = 0;
};
