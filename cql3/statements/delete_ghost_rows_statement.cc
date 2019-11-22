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

#include "cql3/statements/delete_ghost_rows_statement.hh"
#include "transport/messages/result_message.hh"
#include "cql3/selection/selection.hh"
#include "query-result-reader.hh"
#include "view_info.hh"
#include "database.hh"
#include "timeout_config.hh"
#include "service/pager/query_pagers.hh"
#include <boost/range/adaptors.hpp>
#include <boost/range/iterator_range.hpp>

using namespace std::chrono_literals;

namespace cql3 {

namespace statements {

static future<> delete_ghost_rows(dht::partition_range_vector&& partition_ranges, std::vector<query::clustering_range>&& clustering_bounds, view_ptr view,
        service::storage_proxy& proxy, service::query_state& state, const query_options& options, cql_stats& stats, db::timeout_clock::duration timeout_duration) {
    return seastar::async([partition_ranges = std::move(partition_ranges), clustering_bounds = std::move(clustering_bounds),
            view = std::move(view), &proxy, &state, &options, &stats, timeout_duration] () mutable {
        auto key_columns = boost::copy_range<std::vector<const column_definition*>>(
            view->all_columns()
            | boost::adaptors::filtered([] (const column_definition& cdef) { return cdef.is_primary_key(); })
            | boost::adaptors::transformed([] (const column_definition& cdef) { return &cdef; } ));
        auto selection = cql3::selection::selection::for_columns(view, key_columns);

        query::partition_slice partition_slice(std::move(clustering_bounds), {},  {}, selection->get_query_options());
        auto command = ::make_lw_shared<query::read_command>(view->id(), view->version(), partition_slice, query::max_partitions);

        tracing::trace(state.get_trace_state(), "Deleting ghost rows from partition ranges {}", partition_ranges);

        auto p = service::pager::query_pagers::ghost_row_deleting_pager(schema_ptr(view), selection, state,
                options, std::move(command), std::move(partition_ranges), stats, proxy, timeout_duration);

        int32_t page_size = std::max(options.get_page_size(), 1000);
        auto now = gc_clock::now();

        while (!p->is_exhausted()) {
            tracing::trace(state.get_trace_state(), "Fetching a page for ghost row deletion");
            auto timeout = db::timeout_clock::now() + timeout_duration;
            cql3::selection::result_set_builder builder(*selection, now, options.get_cql_serialization_format());
            p->fetch_page(builder, page_size, now, timeout).get();
        }
    });
}

future<::shared_ptr<cql_transport::messages::result_message>> delete_ghost_rows_statement::do_execute(service::storage_proxy& proxy,
                                                                                 service::query_state& state, const query_options& options) const {
    tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());

    if (_restrictions->need_filtering()) {
        throw exceptions::invalid_request_exception("Deleting ghost rows does not support filtering");
    }
    if (!_schema->is_view()) {
        throw exceptions::invalid_request_exception("Ghost rows can only be deleted from materialized views");
    }

    auto timeout_duration = options.get_timeout_config().*cql_statement::get_timeout_config_selector();
    dht::partition_range_vector key_ranges = _restrictions->get_partition_key_ranges(options);
    std::vector<query::clustering_range> clustering_bounds = _restrictions->get_clustering_bounds(options);
    return delete_ghost_rows(std::move(key_ranges), std::move(clustering_bounds), view_ptr(_schema), proxy, state, options, _stats, timeout_duration).then([] {
        return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(::make_shared<cql_transport::messages::result_message::void_message>());
    });
}

}

}
