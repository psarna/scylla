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
#include "row_marker.hh"

class schema;
class partition_key;
class clustering_row;

class column_computation;
using column_computation_ptr = std::unique_ptr<column_computation>;

namespace detail {
using const_iterator_range_type = boost::iterator_range<std::vector<column_definition>::const_iterator>;
}

class token_column_computation {};

class map_value_column_computation {
    bytes _map_name;
    bytes _key;
public:
    map_value_column_computation(bytes map_name, bytes key) : _map_name(map_name), _key(key) { }

    Json::Value to_json() const;
    const bytes& key() const {
        return _key;
    }

    const column_definition& get_map_column(const schema& schema) const;
};

/*
 * Column computation represents a computation performed in order to obtain a value for a computed column.
 * Computed columns description is also available at docs/system_schema_keyspace.md. They hold values
 * not provided directly by the user, but rather computed: from other column values and possibly other sources.
 * This class is able to serialize/deserialize column computations and perform the computation itself,
 * based on given schema, partition key and clustering row. Responsibility for providing enough data
 * in the clustering row in order for computation to succeed belongs to the caller. In particular,
 * generating a value might involve performing a read-before-write if the computation is performed
 * on more values than are present in the update request.
 */
class column_computation final {
public:
    using computation_type = std::variant<token_column_computation, map_value_column_computation>;
private:
    computation_type _computation;
public:
    explicit column_computation(computation_type c) : _computation(c) {}
    ~column_computation() = default;

    static column_computation deserialize(bytes_view raw);
    static column_computation deserialize(const Json::Value& json);

    const computation_type& computation() const {
        return _computation;
    }

    bytes serialize() const;
    bytes_opt compute_value(const schema& schema, const partition_key& key, const clustering_row& row) const;
    detail::const_iterator_range_type dependent_columns(const schema& schema) const;
};
