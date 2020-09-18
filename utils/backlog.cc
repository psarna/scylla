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

#include "utils/backlog.hh"
#include <algorithm>

namespace utils {

std::chrono::microseconds backlog::calculate_delay(budget_clock_type::duration budget) {
    auto adjust = [] (float x) { return x * x * x; };
    std::chrono::microseconds ret(uint32_t(adjust(relative_size()) * delay_limit_us));
    // "budget" has millisecond resolution and can potentially be long
    // in the future so converting it to microseconds may overflow.
    // So to compare buget and ret we need to convert both to the lower
    // resolution.
    if (std::chrono::duration_cast<budget_clock_type::duration>(ret) < budget) {
        return ret;
    } else {
        // budget is small (< ret) so can be converted to microseconds
        return budget;
    }
}

std::chrono::microseconds backlog::calculate_delay() {
    return calculate_delay(std::chrono::duration_cast<budget_clock_type::duration>(
            std::chrono::microseconds(delay_limit_us)));
}

}