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

namespace cql3::restrictions::v2 {

using single_value = ::shared_ptr<term>;
using multiple_values = std::vector<::shared_ptr<term>>;
struct map_entry {
    ::shared_ptr<term> key;
    ::shared_ptr<term> value;
};

class restriction {
public:
    const operator_type& _op;
    std::vector<const column_definition*> _target;
    std::variant<single_value, multiple_values, map_entry, term_slice, ::shared_ptr<abstract_marker>> _value;
    bool _on_token;

    restriction(const operator_type& op) : _op(op), _target(), _value(), _on_token() {}

    sstring to_string() const;
};

static inline std::ostream& operator<<(std::ostream& out, const restriction& r) {
    return out << r.to_string();
}

class prepared_restrictions {
public:
    std::vector<restriction> _restrictions;

    std::optional<secondary_index::index> _index;
    std::vector<const column_definition*> _filtered_columns;

    static prepared_restrictions prepare_restrictions(database& db, const schema& schema, const std::vector<::shared_ptr<relation>>& where_clause, ::shared_ptr<variable_specifications> bound_names);
};

}
