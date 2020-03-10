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

#include "service/storage_proxy.hh"
#include "service/query_state.hh"
#include "query-result-reader.hh"
#include "service/pager/query_pagers.hh"

template<typename UnderlyingVisitor = service::pager::noop_visitor>
class delete_ghost_rows_visitor {
    service::storage_proxy& _proxy;
    service::query_state& _state;
    const cql3::query_options& _options;
    db::timeout_clock::duration _timeout_duration;
    view_ptr _view;
    table& _view_table;
    schema_ptr _base_schema;
    std::optional<partition_key> _view_pk;
    UnderlyingVisitor _underlying_visitor;
public:
    delete_ghost_rows_visitor(service::storage_proxy& proxy, service::query_state& state, view_ptr view, const cql3::query_options& options, db::timeout_clock::duration timeout_duration, UnderlyingVisitor&& visitor)
            : _proxy(proxy)
            , _state(state)
            , _options(options)
            , _timeout_duration(timeout_duration)
            , _view(view)
            , _view_table(_proxy.get_db().local().find_column_family(view))
            , _base_schema(_proxy.get_db().local().find_schema(_view->view_info()->base_id()))
            , _view_pk()
            , _underlying_visitor(std::move(visitor))
    {}

    void add_value(const column_definition& def, query::result_row_view::iterator_type& i) {
        _underlying_visitor.add_value(def, i);
    }

    void accept_new_partition(const partition_key& key, uint32_t row_count) {
        assert(thread::running_in_thread());
        _view_pk = key;
        _underlying_visitor.accept_new_partition(key, row_count);
    }

    void accept_new_partition(uint32_t row_count) {
        _underlying_visitor.accept_new_partition(row_count);
    }

    // Assumes running in seastar::thread
    void accept_new_row(const clustering_key& ck, const query::result_row_view& static_row, const query::result_row_view& row) {
        auto view_exploded_pk = _view_pk->explode();
        auto view_exploded_ck = ck.explode();
        std::vector<bytes> base_exploded_pk(_base_schema->partition_key_size());
        std::vector<bytes> base_exploded_ck(_base_schema->clustering_key_size());
        for (const column_definition& view_cdef : _view->all_columns()) {
            const column_definition* base_cdef = _base_schema->get_column_definition(view_cdef.name());
            if (base_cdef) {
                std::vector<bytes>& view_exploded_key = view_cdef.is_partition_key() ? view_exploded_pk : view_exploded_ck;
                if (base_cdef->is_partition_key()) {
                    base_exploded_pk[base_cdef->id] = view_exploded_key[view_cdef.id];
                } else if (base_cdef->is_clustering_key()) {
                    base_exploded_ck[base_cdef->id] = view_exploded_key[view_cdef.id];
                }
            }
        }
        partition_key base_pk = partition_key::from_exploded(base_exploded_pk);
        clustering_key base_ck = clustering_key::from_exploded(base_exploded_ck);

        dht::partition_range_vector partition_ranges({dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*_base_schema, base_pk))});
        auto selection = cql3::selection::selection::for_columns(_base_schema, std::vector<const column_definition*>({&_base_schema->partition_key_columns().front()}));

        std::vector<query::clustering_range> bounds{query::clustering_range::make_singular(base_ck)};
        query::partition_slice partition_slice(std::move(bounds), {},  {}, selection->get_query_options());
        auto command = ::make_lw_shared<query::read_command>(_base_schema->id(), _base_schema->version(), partition_slice, query::max_partitions);
        auto timeout = db::timeout_clock::now() + _timeout_duration;
        service::storage_proxy::coordinator_query_options opts{timeout, _state.get_permit(), _state.get_client_state(), _state.get_trace_state()};
        auto base_qr = _proxy.query(_base_schema, command, std::move(partition_ranges), db::consistency_level::ALL, opts).get0();
        query::result& result = *base_qr.query_result;
        if (result.row_count().value_or(0) == 0) {
            mutation m(_view, *_view_pk);
            auto& row = m.partition().clustered_row(*_view, ck);
            row.apply(tombstone(api::new_timestamp(), gc_clock::now()));
            timeout = db::timeout_clock::now() + _timeout_duration;
            _proxy.mutate({m}, db::consistency_level::ALL, timeout, _state.get_trace_state(), empty_service_permit()).get();
        } else {
            _underlying_visitor.accept_new_row(ck, static_row, row);
        }
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        _underlying_visitor.accept_new_row(static_row, row);
    }

    uint32_t accept_partition_end(const query::result_row_view& static_row) {
        return _underlying_visitor.accept_partition_end(static_row);
    }
};
