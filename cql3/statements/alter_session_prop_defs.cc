/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
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
    service::client_state::session_params params;
    auto reads = get_simple("latency_limit_for_reads");
    auto writes = get_simple("latency_limit_for_writes");
    auto get_duration = [&] (const sstring& repr) {
        data_value v = duration_type->deserialize(duration_type->from_string(repr));
        cql_duration duration = static_pointer_cast<const duration_type_impl>(duration_type)->from_value(v);
        if (duration.months || duration.days) {
            throw std::runtime_error("Please have mercy and use latency limits smaller than a day!");
        }
        return std::chrono::duration_cast<lowres_clock::duration>(std::chrono::nanoseconds(duration.nanoseconds));
    };
    if (reads) {
	    params.latency_limit_for_reads = get_duration(*reads);
    }
    if (writes) {
	    params.latency_limit_for_writes = get_duration(*writes);
    }
    return params;
}
