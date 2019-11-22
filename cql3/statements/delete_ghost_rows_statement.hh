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

#include "cql3/statements/select_statement.hh"
#include "cql3/selection/selection.hh"

namespace cql3 {

namespace statements {

class delete_ghost_rows_statement : public primary_key_select_statement {
public:
    delete_ghost_rows_statement(schema_ptr schema,
                     uint32_t bound_terms,
                     lw_shared_ptr<const parameters> parameters,
                     ::shared_ptr<selection::selection> selection,
                     ::shared_ptr<restrictions::statement_restrictions> restrictions,
                     ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
                     bool is_reversed,
                     ordering_comparator_type ordering_comparator,
                     ::shared_ptr<term> limit,
                     ::shared_ptr<term> per_partition_limit,
                     cql_stats &stats) : primary_key_select_statement(schema, bound_terms, parameters, selection, restrictions, group_by_cell_indices, is_reversed, ordering_comparator, limit, per_partition_limit, stats) {}

private:
    virtual future<::shared_ptr<cql_transport::messages::result_message>> do_execute(service::storage_proxy& proxy,
                                                                                     service::query_state& state, const query_options& options) const override;
};

}

}
