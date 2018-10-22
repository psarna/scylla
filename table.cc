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
}

future<> table::generate_mv_updates_from_staging_sstables(service::storage_proxy& proxy, const seastar::abort_source& as) {
    return seastar::async([this, &proxy, &as]() mutable {
        for (sstables::shared_sstable sst : _sstables_staging | boost::adaptors::map_values | boost::adaptors::filtered([] (const sstables::shared_sstable& sst) { return sst->marked_for_async_view_update(); })) {
            if (as.abort_requested()) {
                return;
            }
            flat_mutation_reader staging_sstable_reader = sst->read_rows_flat(_schema);
            staging_sstable_reader.consume_in_thread(db::view::view_updating_consumer(_schema, proxy), db::no_timeout);
            move_sstable_from_staging_in_thread(std::move(sst));
        }
    });
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

future<> table::generate_and_propagate_view_updates_without_staging(const schema_ptr& base,
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
        auto reader = this->make_reader_without_staging_sstables(base, pk, slice, service::get_local_sstable_query_read_priority());
        auto base_token = m.token();
        return db::view::generate_view_updates(base,
                std::move(views),
                flat_mutation_reader_from_mutations({std::move(m)}),
                std::move(reader)).then([this, timeout, base_token = std::move(base_token)] (auto&& updates) mutable {
            return seastar::get_units(*_config.view_update_concurrency_semaphore_for_streaming, 1, timeout).then(
                    [this, base_token = std::move(base_token), updates = std::move(updates)] (auto units) mutable {
                db::view::mutate_MV(std::move(base_token), std::move(updates), _view_stats).handle_exception([units = std::move(units)] (auto ignored) { });
            });
        });
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

/**
 * Given an update for the base table, calculates the set of potentially affected views,
 * generates the relevant updates, and sends them to the paired view replicas.
 */
future<row_locker::lock_holder> table::push_view_replica_updates(const schema_ptr& s, const frozen_mutation& fm, db::timeout_clock::time_point timeout) const {
    //FIXME: Avoid unfreezing here.
    auto m = fm.unfreeze(s);
    return push_view_replica_updates(s, std::move(m), timeout);
}

future<row_locker::lock_holder> table::push_view_replica_updates(const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout) const {
    auto& base = schema();
    m.upgrade(base);
    auto views = affected_views(base, m);
    if (views.empty()) {
        return make_ready_future<row_locker::lock_holder>();
    }
    auto cr_ranges = db::view::calculate_affected_clustering_ranges(*base, m.decorated_key(), m.partition(), views);
    if (cr_ranges.empty()) {
        return generate_and_propagate_view_updates(base, std::move(views), std::move(m), { }, timeout).then([] {
                // In this case we are not doing a read-before-write, just a
                // write, so no lock is needed.
                return make_ready_future<row_locker::lock_holder>();
        });
    }
    // We read the whole set of regular columns in case the update now causes a base row to pass
    // a view's filters, and a view happens to include columns that have no value in this update.
    // Also, one of those columns can determine the lifetime of the base row, if it has a TTL.
    auto columns = boost::copy_range<std::vector<column_id>>(
            base->regular_columns() | boost::adaptors::transformed(std::mem_fn(&column_definition::id)));
    query::partition_slice::option_set opts;
    opts.set(query::partition_slice::option::send_partition_key);
    opts.set(query::partition_slice::option::send_clustering_key);
    opts.set(query::partition_slice::option::send_timestamp);
    opts.set(query::partition_slice::option::send_ttl);
    auto slice = query::partition_slice(
            std::move(cr_ranges), { }, std::move(columns), std::move(opts), { }, cql_serialization_format::internal(), query::max_rows);
    // Take the shard-local lock on the base-table row or partition as needed.
    // We'll return this lock to the caller, which will release it after
    // writing the base-table update.
    future<row_locker::lock_holder> lockf = local_base_lock(base, m.decorated_key(), slice.default_row_ranges(), timeout);
    return lockf.then([this, m = std::move(m), slice = std::move(slice), views = std::move(views), base, timeout] (row_locker::lock_holder lock) mutable {
        return generate_and_propagate_view_updates(base, dht::partition_range::make_singular(m.decorated_key()), std::move(slice), std::move(m), std::move(views), timeout).then([lock = std::move(lock)] () mutable {
            return std::move(lock);
        });
    });
}
