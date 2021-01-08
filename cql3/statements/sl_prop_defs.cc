/*
 * Copyright (C) 2021 ScyllaDB
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

#include "cql3/statements/sl_prop_defs.hh"
#include "database.hh"
#include "duration.hh"
#include "concrete_types.hh"
#include <boost/algorithm/string/predicate.hpp>

namespace cql3 {

namespace statements {

void sl_prop_defs::validate() {
    static std::set<sstring> timeout_props {
        "read_timeout", "write_timeout", "range_read_timeout", "counter_write_timeout", "truncate_timeout", "cas_timeout", "other_timeout"
    };
    auto get_duration = [&] (const std::optional<sstring>& repr) -> std::optional<lowres_clock::duration> {
        if (!repr || boost::algorithm::iequals(*repr, "null")) {
            return std::nullopt;
        }
        data_value v = duration_type->deserialize(duration_type->from_string(*repr));
        cql_duration duration = static_pointer_cast<const duration_type_impl>(duration_type)->from_value(v);
        if (duration.months || duration.days) {
            throw exceptions::invalid_request_exception("Timeout values cannot be longer than 24h");
        }
        if (duration.nanoseconds % 1'000'000 != 0) {
            throw exceptions::invalid_request_exception("Timeout values must be expressed in millisecond granularity");
        }
        return std::make_optional(std::chrono::duration_cast<lowres_clock::duration>(std::chrono::nanoseconds(duration.nanoseconds)));
    };

    property_definitions::validate(timeout_props);
    _slo.read_timeout = get_duration(get_simple("read_timeout"));
    _slo.write_timeout = get_duration(get_simple("write_timeout"));
    _slo.range_read_timeout = get_duration(get_simple("range_read_timeout"));
    _slo.counter_write_timeout = get_duration(get_simple("counter_write_timeout"));
    _slo.truncate_timeout = get_duration(get_simple("truncate_timeout"));
    _slo.cas_timeout = get_duration(get_simple("cas_timeout"));
    _slo.other_timeout = get_duration(get_simple("other_timeout"));
}

qos::service_level_options sl_prop_defs::get_service_level_options() const {
    return _slo;
}

}

}
