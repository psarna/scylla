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

namespace db::view {

class view_update_from_staging_generator {
    database& _db;
    service::storage_proxy& _proxy;
public:
    view_update_from_staging_generator(database& db, service::storage_proxy& proxy) : _db(db), _proxy(proxy) { }

    future<> start() {
        return parallel_for_each(_db.get_non_system_column_families(), [this] (lw_shared_ptr<table> table) {
            return table->generate_mv_updates_from_staging_sstables(_proxy);
        });
    }

    future<> stop() {
        return make_ready_future();
    }
};

}
