/*
 * Copyright (C) 2018 ScyllaDB
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

#include "database.hh"
#include "sstables/sstables.hh"
#include "db/view/view_updating_consumer.hh"

namespace db::view {

class view_update_from_staging_generator {
    database& _db;
    service::storage_proxy& _proxy;
    seastar::abort_source _as;
    future<> _started = make_ready_future<>();
    struct sstable_with_table {
        sstables::shared_sstable sst;
        lw_shared_ptr<table> t;
        sstable_with_table(sstables::shared_sstable sst, lw_shared_ptr<table> t) : sst(sst), t(t) {}
    };
    std::deque<sstable_with_table> _sstables_with_tables;
public:
    view_update_from_staging_generator(database& db, service::storage_proxy& proxy) : _db(db), _proxy(proxy), _as() { }

    future<> start() {
        _started = seastar::async([this]() mutable {
            while (!_sstables_with_tables.empty()) {
                auto& entry = _sstables_with_tables.front();
                _sstables_with_tables.pop_front();
                schema_ptr s = entry.t->schema();
                if (_as.abort_requested()) {
                    return;
                }
                flat_mutation_reader staging_sstable_reader = entry.sst->read_rows_flat(s);
                staging_sstable_reader.consume_in_thread(db::view::view_updating_consumer(s, _proxy, entry.sst, _as), db::no_timeout);
                entry.t->move_sstable_from_staging_in_thread(entry.sst);
            }
        });
        return make_ready_future<>();
    }

    future<> stop() {
        _as.request_abort();
        return std::move(_started);
    }

    future<> register_staging_sstable(sstables::shared_sstable sst, lw_shared_ptr<table> table) {
        _sstables_with_tables.emplace_back(std::move(sst), std::move(table));
        if (_started.available()) {
            return start();
        }
        return make_ready_future<>();
    }
};

}
