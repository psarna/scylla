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

#include "database.hh"
#include "sstables/sstables.hh"
#include "sstables/remove.hh"
#include "db/view/view_updating_consumer.hh"
#include <boost/range/adaptors.hpp>

static logging::logger tlogger("table");

void table::move_sstable_from_staging_in_thread(sstables::shared_sstable sst) {
    auto gen = calculate_generation_for_new_table();
    try {
        sst->move_to_new_dir_in_thread(dir(), gen);
    } catch (...) {
        tlogger.warn("Failed to move sstable {} from staging: {}", sst->get_filename(), std::current_exception());
        return;
    }
    _sstables_staging.erase(sst->generation());
}

future<> table::generate_mv_updates_from_staging_sstables(service::storage_proxy& proxy) {
    return seastar::async([this, &proxy]() mutable {
        for (sstables::shared_sstable sst : _sstables_staging | boost::adaptors::map_values) {
            flat_mutation_reader staging_sstable_reader = sst->read_rows_flat(_schema);
            staging_sstable_reader.consume_in_thread(db::view::view_updating_consumer(_schema, proxy), db::no_timeout);
            move_sstable_from_staging_in_thread(std::move(sst));
        }
    });
}
