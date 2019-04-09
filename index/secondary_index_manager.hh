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

#pragma once

#include "schema.hh"

#include "cql3/operator.hh"

#include <vector>
#include <set>

namespace cql3::restrictions {
class restrictions;
}

namespace secondary_index {

sstring index_table_name(const sstring& index_name);

class index {
    sstring _target_column;
    index_metadata _im;
public:
    using score = int;

    index(const sstring& target_column, const index_metadata& im);
    bool depends_on(const column_definition& cdef) const;
    bool supports_expression(const column_definition& cdef, const cql3::operator_type& op) const;
    const index_metadata& metadata() const;
    const sstring& target_column() const {
        return _target_column;
    }

    score fulfills_restrictions(const cql3::restrictions::restrictions& restrictions);
};

class secondary_index_manager {
    column_family& _cf;
    /// The key of the map is the name of the index as stored in system tables.
    std::unordered_map<sstring, index> _indices;
public:
    secondary_index_manager(column_family& cf);
    void reload();
    const schema& get_schema() const;
    view_ptr create_view_for_index(const index_metadata& index) const;
    std::vector<index_metadata> get_dependent_indices(const column_definition& cdef) const;
    std::vector<index> list_indexes() const;
    bool is_index(view_ptr) const;
    bool is_index(const schema& s) const;
private:
    void add_index(const index_metadata& im);
};

}
