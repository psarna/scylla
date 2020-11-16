/*
 * Copyright (C) 2020 ScyllaDB
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

#include "property_definitions.hh"
#include "service/client_state.hh"
#include <seastar/core/sstring.hh>

#include <unordered_map>
#include <optional>

namespace cql3 {

namespace statements {

class alter_session_prop_defs : public property_definitions {
public:
    void validate();
    service::client_state::session_params get_params();
    std::map<sstring, sstring> get_raw_params();

    static service::client_state::session_params get_params(const std::map<sstring, sstring>& params);
};

}
}

