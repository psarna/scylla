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

#include "db/system_distributed_keyspace.hh"
#include "streaming/stream_reason.hh"
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>

namespace db::view {

inline future<bool> check_view_build_ongoing(db::system_distributed_keyspace& sys_dist_ks, const sstring& ks_name, const sstring& cf_name) {
    return sys_dist_ks.view_status(ks_name, cf_name).then([] (std::unordered_map<utils::UUID, sstring>&& view_statuses) {
        return boost::algorithm::any_of(view_statuses | boost::adaptors::map_values, [] (const sstring& view_status) {
            return view_status == "STARTED";
        });
    });
}

inline future<bool> check_needs_view_update_path(db::system_distributed_keyspace& sys_dist_ks, const table& t, streaming::stream_reason reason) {
    if (is_internal_keyspace(t.schema()->ks_name())) {
        return make_ready_future<bool>(false);
    }
    if (reason == streaming::stream_reason::repair && !t.views().empty()) {
        return make_ready_future<bool>(true);
    }
    return do_with(t.views(), [&sys_dist_ks] (auto& views) {
        return map_reduce(views,
                [&sys_dist_ks] (const view_ptr& view) { return check_view_build_ongoing(sys_dist_ks, view->ks_name(), view->cf_name()); },
                false,
                std::logical_or<bool>());
    });
}

}
