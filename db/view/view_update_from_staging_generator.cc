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

#include "view_update_from_staging_generator.hh"

namespace db::view {


future<> view_update_from_staging_generator::start() {
    _started = seastar::async([this]() mutable {
        while (!_sstables_with_tables.empty()) {
            auto& entry = _sstables_with_tables.front();
            schema_ptr s = entry.t->schema();
            if (_as.abort_requested()) {
                return;
            }
            flat_mutation_reader staging_sstable_reader = entry.sst->read_rows_flat(s);
            auto result = staging_sstable_reader.consume_in_thread(view_updating_consumer(s, _proxy, entry.sst, _as), db::no_timeout);
            if (result == stop_iteration::no) {
                entry.t->move_sstable_from_staging_in_thread(entry.sst);
                _registration_sem.signal();
                _sstables_with_tables.pop_front();
            }
        }
    });
    return make_ready_future<>();
}

future<> view_update_from_staging_generator::stop() {
    _as.request_abort();
    return std::move(_started);
}

future<> view_update_from_staging_generator::register_staging_sstable(sstables::shared_sstable sst, lw_shared_ptr<table> table) {
    _sstables_with_tables.emplace_back(std::move(sst), std::move(table));
    if (_as.abort_requested()) {
        return make_ready_future<>();
    }
    future<> restart = make_ready_future<>();
    if (_started.available()) {
        restart = start();
    }
    return restart.then([this] () {
        return _registration_sem.wait(1);
    });
}

}
