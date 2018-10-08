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

void table::mark_sstable_needs_mv_update_generation(sstables::shared_sstable sst) {
    _sstables_staging_need_mv_update_generation.emplace(sst->generation(), sst);
}

void table::unmark_sstable_needs_mv_update_generation(sstables::shared_sstable sst) {
    _sstables_staging_need_mv_update_generation.erase(sst->generation());
}

future<> table::move_sstable_from_staging(sstables::shared_sstable sst) {
    auto gen = calculate_generation_for_new_table();
    return sst->read_toc().then([sst] {
        return sst->mutate_sstable_level(0);
    }).then([this, sst, gen] {
        sst->set_dir(dir());
        return sst->set_generation(gen).handle_exception([sst] (std::exception_ptr eptr) {
            tlogger.warn("Failed to move sstable {} from staging: {}", sst->get_filename(), eptr);
        });
    }).then([this, sst]() mutable {
        return get_row_cache().invalidate([this, sst] {
            _sstables_staging.erase(sst->generation());
        });
    });
}

future<> table::generate_mv_updates_from_staging_sstables(service::storage_proxy& proxy) {
    return do_for_each(get_sstables_for_mv_update_generation(), [&proxy, this] (const std::pair<uint64_t, sstables::shared_sstable>& sstable_entry) {
        sstables::shared_sstable sst = sstable_entry.second;
        flat_mutation_reader staging_sstable_reader = sst->read_rows_flat(_schema);
        return seastar::async([this, &proxy, staging_sstable_reader = std::move(staging_sstable_reader)]() mutable {
            staging_sstable_reader.consume_in_thread(db::view::view_updating_consumer(_schema, proxy), db::no_timeout);
        }).then([this, sst = std::move(sst)] {
            return move_sstable_from_staging(std::move(sst));
        });
    });
}
