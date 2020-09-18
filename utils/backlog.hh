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

#include <cstddef>
#include <limits>
#include <chrono>
#include <seastar/core/lowres_clock.hh>

namespace utils {

/**
 * The view update backlog represents the pending view data that a base replica
 * maintains. It is the maximum of the memory backlog - how much memory pending
 * view updates are consuming out of the their allocated quota - and the disk
 * backlog - how much view hints are consuming. The size of a backlog is relative
 * to its maximum size.
 */
struct backlog {
    static constexpr size_t delay_limit_us = 1000000;
    using budget_clock_type = seastar::lowres_clock;

    size_t current;
    size_t max;

    float relative_size() const {
        return float(current) / float(max);
    }

    friend bool operator==(const backlog& lhs, const backlog& rhs) {
        return lhs.relative_size() == rhs.relative_size();
    }

    friend bool operator<(const backlog& lhs, const backlog& rhs) {
        return lhs.relative_size() < rhs.relative_size();
    }

    friend bool operator!=(const backlog& lhs, const backlog& rhs) {
        return !(lhs == rhs);
    }

    friend bool operator<=(const backlog& lhs, const backlog& rhs) {
        return !(rhs < lhs);
    }

    friend bool operator>(const backlog& lhs, const backlog& rhs) {
        return rhs < lhs;
    }

    friend bool operator>=(const backlog& lhs, const backlog& rhs) {
        return !(lhs < rhs);
    }

    static backlog no_backlog() {
        return backlog{0, std::numeric_limits<size_t>::max()};
    }

    // Calculates how much to delay completing the request. The delay adds to the request's inherent latency.
    std::chrono::microseconds calculate_delay(budget_clock_type::duration budget);
    std::chrono::microseconds calculate_delay();
};

}