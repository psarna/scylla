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

#include "database_fwd.hh"
#include "sstables/sstables.hh"
#include "db/view/view_updating_consumer.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/semaphore.hh>

namespace db::view {

/**
 * The view_update_generator is a sharded service responsible for generating view updates
 * from sstables that need it, which includes:
 *  - sstables streamed via repair
 *  - sstables streamed during view building
 *  - sstables loaded via `nodetool refresh` and /upload directory
 *
 * SSTables can be registered either offline (before the service is started), e.g. during
 * initial directory scans, or online, which happens e.g. during streaming.
 * For consistency reasons, SSTables that require view update generation reside in directories
 * different than the regular data path for a table (/staging, /upload, etc.).
 * These sstables do not take part in compaction (so they can be easily tracked) and they
 * are not used during view update generation for other sstables belonging to the same table.
 *
 * After an sstable is registered to the update generator, it is queued for view update
 * generation. Later, it is moved from its temporary location to its target data directory
 * and becomes a first class citizen, which means that it can be compacted, read from,
 * used as a source for another view update generation process, and so on.
 *
 * In order to prevent too many in-flight view updates (e.g. when a large number of sstables
 * is streamed in a short period of time), the registration queue is throttled with a semaphore
 * that accepts up to 5 active waiters.
 */
class view_update_generator {
    static constexpr size_t registration_queue_size = 5;
    database& _db;
    service::storage_proxy& _proxy;
    seastar::abort_source _as;
    future<> _started = make_ready_future<>();
    seastar::condition_variable _pending_sstables;
    semaphore _registration_sem{registration_queue_size};
    struct sstable_with_table {
        sstables::shared_sstable sst;
        lw_shared_ptr<table> t;
        sstable_with_table(sstables::shared_sstable sst, lw_shared_ptr<table> t) : sst(sst), t(t) { }
    };
    std::deque<sstable_with_table> _sstables_with_tables;
public:
    view_update_generator(database& db, service::storage_proxy& proxy) : _db(db), _proxy(proxy) { }

    future<> start();
    future<> stop();
    future<> register_sstable(sstables::shared_sstable sst, lw_shared_ptr<table> table);
private:
    bool should_throttle() const;
};

}
