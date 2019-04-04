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
class column_computation {
public:
    using const_iterator_range_type = boost::iterator_range<std::vector<column_definition>::const_iterator>;

    virtual ~column_computation() = default;

    static column_computation_ptr deserialize(bytes_view raw);
    static column_computation_ptr deserialize(const Json::Value& json);

    virtual column_computation_ptr clone() const = 0;

    virtual bytes serialize() const = 0;
    virtual bytes_opt compute_value(const schema& schema, const partition_key& key, const clustering_row& row) const = 0;
    virtual row_marker compute_row_marker(const schema& schema, const clustering_row& row) const = 0;
    virtual const_iterator_range_type dependent_columns(const schema& schema) const = 0;
};

class token_column_computation : public column_computation {
public:
    virtual column_computation_ptr clone() const override {
        return std::make_unique<token_column_computation>(*this);
    }
    virtual bytes serialize() const override;
    virtual bytes_opt compute_value(const schema& schema, const partition_key& key, const clustering_row& row) const override;
    virtual row_marker compute_row_marker(const schema& schema, const clustering_row& row) const override;
    virtual const_iterator_range_type dependent_columns(const schema& schema) const override;
};

class map_value_column_computation : public column_computation {
    bytes _map_name;
    bytes _key;
public:
    map_value_column_computation(bytes map_name, bytes key) : _map_name(map_name), _key(key) { }

    virtual column_computation_ptr clone() const override {
        return std::make_unique<map_value_column_computation>(*this);
    }
    virtual bytes serialize() const override;
    virtual bytes_opt compute_value(const schema& schema, const partition_key& key, const clustering_row& row) const override;
    virtual row_marker compute_row_marker(const schema& schema, const clustering_row& row) const override;
    virtual const_iterator_range_type dependent_columns(const schema& schema) const override;

    Json::Value to_json() const;
    const bytes& key() const {
        return _key;
    }

protected:
    const column_definition& get_map_column(const schema& schema) const;
};
