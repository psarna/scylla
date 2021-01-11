/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
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
        if (!repr) {
            return std::nullopt;
        }
        if (boost::algorithm::iequals(*repr, "null")) {
            return qos::service_level_options::delete_marker;
        }
        data_value v = duration_type->deserialize(duration_type->from_string(*repr));
        cql_duration duration = static_pointer_cast<const duration_type_impl>(duration_type)->from_value(v);
        if (duration.months || duration.days) {
            throw exceptions::invalid_request_exception("Timeout values cannot be longer than 24h");
        }
        if (duration.nanoseconds % 1'000'000 != 0) {
            throw exceptions::invalid_request_exception("Timeout values must be expressed in millisecond granularity");
        }
        auto ret = std::chrono::duration_cast<lowres_clock::duration>(std::chrono::nanoseconds(duration.nanoseconds));
        if (ret == qos::service_level_options::delete_marker) {
            throw exceptions::invalid_request_exception(format("Timeout value {}ns is reserved and cannot be explicitly used", duration.nanoseconds));
        }
        return ret;
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
