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

#include "qos_common.hh"
namespace qos {

service_level_options service_level_options::replace_defaults(const service_level_options& other) const {
    service_level_options ret = *this;
    auto maybe_replace = [&ret, &other] (std::optional<lowres_clock::duration> service_level_options::*member) mutable {
        if (ret.*member == delete_marker) {
            ret.*member = std::nullopt;
        } else if (!(ret.*member)) {
            ret.*member = other.*member;
        }
    };
    maybe_replace(&service_level_options::timeout);
    return ret;
}

service_level_options service_level_options::merge_with(const service_level_options& other) const {
    service_level_options ret = *this;
    auto min_of = [&ret, &other] (std::optional<lowres_clock::duration> service_level_options::*member) mutable {
        if (!(ret.*member)) {
            ret.*member = other.*member;
        } else if (other.*member) {
            ret.*member = std::min(*(ret.*member), *(other.*member));
        }
    };
    min_of(&service_level_options::timeout);
    return ret;
}

}
