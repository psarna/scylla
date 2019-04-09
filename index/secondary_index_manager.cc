/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2017 ScyllaDB
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

#include "index/secondary_index_manager.hh"

#include "cql3/statements/index_target.hh"
#include "cql3/util.hh"
#include "index/target_parser.hh"
#include "db/query_context.hh"
#include "schema_builder.hh"
#include "database.hh"

#include "service/storage_service.hh"

#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>

namespace secondary_index {

index::index(const sstring& target_column, const index_metadata& im)
    : _target_column{target_column}
    , _im{im}
{}

bool index::depends_on(const column_definition& cdef) const {
    return cdef.name_as_text() == _target_column;
}

bool index::supports_expression(const column_definition& cdef, const cql3::operator_type& op) const {
    return cdef.name_as_text() == _target_column && op == cql3::operator_type::EQ;
}

const index_metadata& index::metadata() const {
    return _im;
}

index::score fulfills_restrictions(const cql3::restrictions::restrictions& restrictions) {
    //FIXME(sarna): check all for the longest prefix and return its length as a score
    return 0;
}

secondary_index_manager::secondary_index_manager(column_family& cf)
    : _cf{cf}
{}

void secondary_index_manager::reload() {
    const auto& table_indices = _cf.schema()->all_indices();
    auto it = _indices.begin();
    while (it != _indices.end()) {
        auto index_name = it->first;
        if (table_indices.count(index_name) == 0) {
            it = _indices.erase(it);
        } else {
            ++it;
        }
    }
    for (const auto& index : _cf.schema()->all_indices()) {
        add_index(index.second);
    }
}

void secondary_index_manager::add_index(const index_metadata& im) {
    sstring index_target = im.options().at(cql3::statements::index_target::target_option_name);
    sstring index_target_name = target_parser::get_target_column_name_from_string(index_target);
    _indices.emplace(im.name(), index{index_target_name, im});
}

sstring index_table_name(const sstring& index_name) {
    return format("{}_index", index_name);
}

static bytes get_available_token_column_name(const schema& schema) {
    bytes base_name = "idx_token";
    bytes accepted_name = base_name;
    int i = 0;
    while (schema.get_column_definition(accepted_name)) {
        accepted_name = base_name + to_bytes("_")+ to_bytes(std::to_string(++i));
    }
    return accepted_name;
}

const schema& secondary_index_manager::get_schema() const {
    return *_cf.schema();
}

view_ptr secondary_index_manager::create_view_for_index(const index_metadata& im) const {
    auto schema = _cf.schema();
    sstring index_target_name = im.options().at(cql3::statements::index_target::target_option_name);
    schema_builder builder{schema->ks_name(), index_table_name(im.name())};
    auto target_info = im.get_target_info(*schema);
    const column_definition* index_target = im.get_target_column(*schema);
    auto target_type = target_info.type;
    if (target_type != cql3::statements::index_target::target_type::values) {
        throw std::runtime_error(format("Unsupported index target type: {}", to_sstring(target_type)));
    }

    for (const column_definition* cdef : target_info.pk_columns) {
        builder.with_column(cdef->name(), cdef->type, column_kind::partition_key, 0, cdef->view_virtual(), cdef->get_computation_ptr());
    }
    for (const column_definition* cdef : target_info.ck_columns) {
        builder.with_column(cdef->name(), cdef->type, column_kind::clustering_key, 0, cdef->view_virtual(), cdef->get_computation_ptr());
    }
    if (index_target->is_primary_key()) {
        for (auto& def : schema->regular_columns()) {
            db::view::create_virtual_column(builder, def.name(), def.type);
        }
    }
    const sstring restricted_column = index_target->is_computed() ? target_parser::get_target_column_name_from_string(im.options().at(cql3::statements::index_target::target_option_name)) : index_target->name_as_cql_string();
    const sstring where_clause = format("{} IS NOT NULL", restricted_column);
    builder.with_view_info(*schema, false, where_clause);
    return view_ptr{builder.build()};
}

std::vector<index_metadata> secondary_index_manager::get_dependent_indices(const column_definition& cdef) const {
    return boost::copy_range<std::vector<index_metadata>>(_indices
           | boost::adaptors::map_values
           | boost::adaptors::filtered([&] (auto& index) { return index.depends_on(cdef); })
           | boost::adaptors::transformed([&] (auto& index) { return index.metadata(); }));
}

std::vector<index> secondary_index_manager::list_indexes() const {
    return boost::copy_range<std::vector<index>>(_indices | boost::adaptors::map_values);
}

bool secondary_index_manager::is_index(view_ptr view) const {
    return is_index(*view);
}

bool secondary_index_manager::is_index(const schema& s) const {
    return boost::algorithm::any_of(_indices | boost::adaptors::map_values, [&s] (const index& i) {
        return s.cf_name() == index_table_name(i.metadata().name());
    });
}

}
