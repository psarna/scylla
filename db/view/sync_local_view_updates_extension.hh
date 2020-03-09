/*
 * Copyright 2020 ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "serializer.hh"
#include "db/extensions.hh"

namespace db::view {

class sync_local_view_updates_extension : public schema_extension {
    bool _enabled = false;
public:
    static constexpr auto NAME = "sync_local_view_updates";

    sync_local_view_updates_extension() = default;
    explicit sync_local_view_updates_extension(bool enabled) : _enabled(enabled) {}
    explicit sync_local_view_updates_extension(std::map<sstring, sstring>) {
        throw std::logic_error("sync_local_view_updates cannot be initialized with a map of options");
    }
    explicit sync_local_view_updates_extension(bytes b) : _enabled(deserialize(b)) {}
    explicit sync_local_view_updates_extension(const sstring& s) {
        if (s == "true") {
            _enabled = true;
        } else if (s != "false") {
            throw std::runtime_error("sync_local_view_updates can only be set to true or false");
        }
    }
    bytes serialize() const override {
        return ser::serialize_to_buffer<bytes>(_enabled);
    }
    static bool deserialize(bytes_view buffer) {
        return ser::deserialize_from_buffer(buffer, boost::type<bool>());
    }
    bool enabled() const {
        return _enabled;
    }
};

}
