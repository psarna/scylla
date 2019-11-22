/*
 * Copyright (C) 2015 ScyllaDB
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
extern logging::logger dblog;
class regenerate_views_visitor {
    service::storage_proxy& _proxy;
    schema_ptr _schema;
    table& _table;
    std::optional<partition_key> _pk;
    db::timeout_clock::duration _timeout_duration;
public:
    regenerate_views_visitor(service::storage_proxy& proxy, schema_ptr schema, db::timeout_clock::duration timeout_duration)
            : _proxy(proxy)
            , _schema(schema)
            , _table(_proxy.get_db().local().find_column_family(schema))
            , _pk()
            , _timeout_duration(timeout_duration)
    {}

    void add_value(const column_definition& def, query::result_row_view::iterator_type& i) {
    }

    void accept_new_partition(const partition_key& key, uint32_t row_count) {
        assert(thread::running_in_thread());
        _pk = key;
    }

    void accept_new_partition(uint32_t row_count) {
    }

    // Assumes running in seastar::thread
    void accept_new_row(const clustering_key& ck, const query::result_row_view& static_row, const query::result_row_view& row) {
        mutation m(_schema, *_pk);
        auto& clustered_row = m.partition().clustered_row(*_schema, ck);

        auto row_iterator = row.iterator();
        auto static_row_iterator = static_row.iterator();
        for (const auto& def : _schema->all_columns()) {
            switch (def.kind) {
            case column_kind::partition_key:
                dblog.warn("pk {}", def.name_as_text());
                break;
            case column_kind::clustering_key:
                dblog.warn("ck {}", def.name_as_text());
                break;
            case column_kind::regular_column:
                dblog.warn("regular {}", def.name_as_text());
                if (def.is_atomic()) {
                    auto cell = row_iterator.next_atomic_cell();
                    if (cell) {
                        auto expiry = cell->expiry();
                        if (expiry) {
                            auto ttl = cell->ttl();
                            clustered_row.cells().apply(def, atomic_cell::make_live(*def.type, api::new_timestamp(), cell->value(), *expiry, *ttl));
                        } else {
                            clustered_row.cells().apply(def, atomic_cell::make_live(*def.type, api::new_timestamp(), cell->value()));
                        }
                    }
                } else {
                    //FIXME(sarna): Support collections
                    row_iterator.next_collection_cell();
                }
                break;
            case column_kind::static_column:
                break;
            }
        }

        auto timeout = db::timeout_clock::now() + _timeout_duration;
        dblog.warn("Sending {}", m);
        const bool force_updates = true;
        auto lock_holder = _table.push_view_replica_updates(_schema, std::move(m), timeout, force_updates).get0();
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
    }

    uint32_t accept_partition_end(const query::result_row_view& static_row) {
        return 0;
    }
};
