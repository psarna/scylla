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
#include <service/priority_manager.hh>
#include <boost/range/adaptors.hpp>

static logging::logger tlogger("table");

void table::move_sstable_from_staging_in_thread(sstables::shared_sstable sst) {
    try {
        sst->move_to_new_dir_in_thread(dir(), sst->generation());
    } catch (...) {
        tlogger.warn("Failed to move sstable {} from staging: {}", sst->get_filename(), std::current_exception());
        return;
    }
    _sstables_staging.erase(sst->generation());
    _compaction_strategy.get_backlog_tracker().add_sstable(sst);
}

future<> table::generate_and_propagate_view_updates(const schema_ptr& base,
        dht::partition_range&& pk,
        query::partition_slice&& slice,
        mutation&& m,
        std::vector<view_ptr>&& views,
        db::timeout_clock::time_point timeout) const {
        return do_with(
                std::move(pk),
                std::move(slice),
                std::move(m),
                [this, base, views = std::move(views), timeout] (dht::partition_range& pk, query::partition_slice& slice, mutation& m) mutable {
            auto reader = this->make_reader(base, pk, slice, service::get_local_sstable_query_read_priority());
            return generate_and_propagate_view_updates(base, std::move(views), std::move(m), std::move(reader), timeout);
        });
}

/**
 * Given some updates on the base table and the existing values for the rows affected by that update, generates the
 * mutations to be applied to the base table's views, and sends them to the paired view replicas.
 *
 * @param base the base schema at a particular version.
 * @param views the affected views which need to be updated.
 * @param updates the base table updates being applied.
 * @param existings the existing values for the rows affected by updates. This is used to decide if a view is
 * obsoleted by the update and should be removed, gather the values for columns that may not be part of the update if
 * a new view entry needs to be created, and compute the minimal updates to be applied if the view entry isn't changed
 * but has simply some updated values.
 * @return a future resolving to the mutations to apply to the views, which can be empty.
 */
future<> table::generate_and_propagate_view_updates(const schema_ptr& base,
        std::vector<view_ptr>&& views,
        mutation&& m,
        flat_mutation_reader_opt existings,
        db::timeout_clock::time_point timeout) const {
    auto base_token = m.token();
    return db::view::generate_view_updates(base,
            std::move(views),
            flat_mutation_reader_from_mutations({std::move(m)}),
            std::move(existings)).then([this, timeout, base_token = std::move(base_token)] (auto&& updates) mutable {
        return seastar::get_units(*_config.view_update_concurrency_semaphore, 1, timeout).then(
                [this, base_token = std::move(base_token), updates = std::move(updates)] (auto units) mutable {
            db::view::mutate_MV(std::move(base_token), std::move(updates), _view_stats).handle_exception([units = std::move(units)] (auto ignored) { });
        });
    });
}
