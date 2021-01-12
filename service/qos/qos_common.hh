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

#include "seastarx.hh"
#include <seastar/core/sstring.hh>
#include <seastar/core/print.hh>
#include <map>
#include <stdexcept>
#include <optional>
#include <seastar/core/lowres_clock.hh>

namespace qos {

/**
 *  a structure that holds the configuration for
 *  a service level.
 */
struct service_level_options {
    static constexpr lowres_clock::duration delete_marker = lowres_clock::duration::max();

    std::optional<lowres_clock::duration> read_timeout = {};
    std::optional<lowres_clock::duration> write_timeout = {};
    std::optional<lowres_clock::duration> range_read_timeout = {};
    std::optional<lowres_clock::duration> counter_write_timeout = {};
    std::optional<lowres_clock::duration> truncate_timeout = {};
    std::optional<lowres_clock::duration> cas_timeout = {};
    std::optional<lowres_clock::duration> other_timeout = {};

    service_level_options replace_defaults(const service_level_options& other) const;

    bool operator==(const service_level_options& other) const = default;
    bool operator!=(const service_level_options& other) const = default;
};

using service_levels_info = std::map<sstring, service_level_options>;

///
/// A logical argument error for a service_level statement operation.
///
class service_level_argument_exception : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

///
/// An exception to indicate that the service level given as parameter doesn't exist.
///
class nonexistant_service_level_exception : public service_level_argument_exception {
public:
    nonexistant_service_level_exception(sstring service_level_name)
            : service_level_argument_exception(format("Service Level {} doesn't exists.", service_level_name)) {
    }
};

}
