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

#include "query-result-reader.hh"

namespace service {
class storage_proxy;
class query_state;
}

namespace db::view {

class delete_ghost_rows_visitor {
    service::storage_proxy& _proxy;
    service::query_state& _state;
    const cql3::query_options& _options;
    db::timeout_clock::duration _timeout_duration;
    view_ptr _view;
    table& _view_table;
    schema_ptr _base_schema;
    std::optional<partition_key> _view_pk;
public:
    delete_ghost_rows_visitor(service::storage_proxy& proxy, service::query_state& state, view_ptr view, const cql3::query_options& options, db::timeout_clock::duration timeout_duration);

    void add_value(const column_definition& def, query::result_row_view::iterator_type& i) {
    }

    void accept_new_partition(const partition_key& key, uint32_t row_count);

    void accept_new_partition(uint32_t row_count) {
    }

    // Assumes running in seastar::thread
    void accept_new_row(const clustering_key& ck, const query::result_row_view& static_row, const query::result_row_view& row);

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
    }

    uint32_t accept_partition_end(const query::result_row_view& static_row) {
        return 0;
    }
};

} //namespace db::view
