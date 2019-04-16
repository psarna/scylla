/*
 * Copyright (C) 2019 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "schema.hh"
#include "cql3/term.hh"
#include "cql3/variable_specifications.hh"
#include "cql3/restrictions/term_slice.hh"
#include "cql3/abstract_marker.hh"
#include "index/secondary_index_manager.hh"
#include "cql3/relation.hh"
#include "cql3/single_column_relation.hh"
#include "cql3/multi_column_relation.hh"
#include "cql3/token_relation.hh"
#include "cql3/query_options.hh"

namespace cql3::restrictions::v2 {

using single_value = ::shared_ptr<term>;
using multiple_values = std::vector<::shared_ptr<term>>;
//using marker = ::shared_ptr<abstract_marker>;
struct map_entry {
    ::shared_ptr<term> key;
    ::shared_ptr<term> value;
};

class restriction {
public:
    const operator_type *_op;
    std::vector<const column_definition*> _target;
    //std::variant<single_value, multiple_values, map_entry, term_slice, marker> _value;
    std::variant<single_value, multiple_values, map_entry, term_slice> _value;
    bool _on_token;

    restriction(const operator_type& op) : _op(&op), _target(), _value(), _on_token() {}
    restriction(restriction&& other) noexcept = default;
    restriction& operator=(restriction&& other) noexcept = default;

    sstring to_string() const;
    bool depends_on(const column_definition& column) const;
    bool depends_on_pk() const;
    bool depends_on_ck() const;
    bool depends_on_regular_column() const;
    bool on_token() const {
        return _on_token;
    }
};

static inline std::ostream& operator<<(std::ostream& out, const restriction& r) {
    return out << r.to_string();
}

class prepared_restrictions {
public:
    schema_ptr _schema;
    std::vector<restriction> _restrictions;

    std::optional<secondary_index::index> _index;
    std::vector<const column_definition*> _filtered_columns;

    static prepared_restrictions prepare_restrictions(database& db, schema_ptr schema, const std::vector<::shared_ptr<relation>>& where_clause, ::shared_ptr<variable_specifications> bound_names);

    bool need_filtering() const;
    bool uses_indexing() const;
    bool all_satisfy(std::function<bool(const restriction&)>&& cond) const;
    dht::partition_range_vector get_partition_key_ranges(const query_options& options) const;
    query::clustering_row_ranges get_clustering_bounds(const query_options& options) const;
    size_t size() const {
        return _restrictions.size();
    }

    auto pk_restrictions() const {
        return _restrictions | boost::adaptors::filtered([] (const restriction& r) {
            return r.depends_on_pk();
        });
    }

    auto ck_restrictions() const {
        return _restrictions | boost::adaptors::filtered([] (const restriction& r) {
            return r.depends_on_ck();
        });
    }

    auto regular_column_restrictions() const {
        return _restrictions | boost::adaptors::filtered([] (const restriction& r) {
            return r.depends_on_regular_column();
        });
    }

    bool has_pk_restrictions() const {
        return !pk_restrictions().empty();
    }

    bool has_ck_restrictions() const {
        return !ck_restrictions().empty();
    }

    bool has_regular_column_restrictions() const {
        return !regular_column_restrictions().empty();
    }

    bool is_key_range() const;

    bool key_is_in_relation() const {
        return boost::algorithm::any_of(pk_restrictions(), [&] (const restriction& r) {
            return r._op == &operator_type::IN;
        });
    }

    bool needs_filtering(const restriction& r) const {
        return boost::algorithm::any_of(_filtered_columns, [&r] (const column_definition* cdef) {
            return boost::range::find(r._target, cdef) != r._target.end();
        });
    }

//private:
    prepared_restrictions(schema_ptr schema) : _schema(schema) {}
};

}
