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

#include "alter_session_prop_defs.hh"
#include "types.hh"
#include "concrete_types.hh"

void cql3::statements::alter_session_prop_defs::validate() {
    static const std::set<sstring> keywords({"latency_limit_for_reads", "latency_limit_for_writes"});
    property_definitions::validate(keywords);
}

service::client_state::session_params
cql3::statements::alter_session_prop_defs::get_params() {
    return get_params(get_raw_params());
}

std::map<sstring, sstring> cql3::statements::alter_session_prop_defs::get_raw_params() {
    std::map<sstring, sstring> raw_params;
    for (auto& property : _properties) {
        std::optional<sstring> value = get_simple(property.first);
        if (value) {
            raw_params.emplace(property.first, *value);
        }
    }
    return raw_params;
}

service::client_state::session_params
cql3::statements::alter_session_prop_defs::get_params(const std::map<sstring, sstring>& raw_params) {
    service::client_state::session_params params;
    auto reads = raw_params.find("latency_limit_for_reads");
    auto writes = raw_params.find("latency_limit_for_writes");
    auto get_duration = [&] (const sstring& repr) {
        data_value v = duration_type->deserialize(duration_type->from_string(repr));
        cql_duration duration = static_pointer_cast<const duration_type_impl>(duration_type)->from_value(v);
        if (duration.months || duration.days) {
            throw std::runtime_error("Please have mercy and use latency limits smaller than a day!");
        }
        return std::chrono::duration_cast<lowres_clock::duration>(std::chrono::nanoseconds(duration.nanoseconds));
    };
    if (reads != raw_params.end()) {
	    params.latency_limit_for_reads = get_duration(reads->second);
    }
    if (writes != raw_params.end()) {
	    params.latency_limit_for_writes = get_duration(writes->second);
    }
    return params;
}
