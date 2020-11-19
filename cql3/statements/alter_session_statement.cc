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
 * Copyright (C) 2015 ScyllaDB
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

#include "cql3/statements/alter_session_statement.hh"
#include "db/system_keyspace.hh"
#include "db/query_context.hh"

#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

alter_session_statement::alter_session_statement(::shared_ptr<alter_session_prop_defs> props)
        : cql_statement_no_metadata(&timeout_config::other_timeout)
        , props(props)
{
}

uint32_t alter_session_statement::get_bound_terms() const {
    return 0;
}

bool alter_session_statement::depends_on_keyspace(const sstring& ks_name) const
{
    return false;
}

bool alter_session_statement::depends_on_column_family(const sstring& cf_name) const
{
    return false;
}

future<> alter_session_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const
{
    state.validate_login();
    return make_ready_future<>();
}

void alter_session_statement::validate(service::storage_proxy&, const service::client_state& state) const
{
    props->validate();
}

static future<> update_system_clients_table(service::client_state& state) {
        // FIXME: consider prepared statement
    const static sstring req = format("UPDATE system.{} SET {} = ? WHERE address = ? AND port = ?;",
            db::system_keyspace::CLIENTS);
    return db::execute_cql(req,
            params.to_map(),
            state.get_client_address(),
            state.get_client_port())
        .discard_result();
}

future<::shared_ptr<cql_transport::messages::result_message>>
alter_session_statement::execute(service::storage_proxy& proxy, service::query_state& state, const query_options& options) const {
    state.get_client_state().set_session_params(props->get_params());
    return update_system_clients_table(state.get_client_state(), props->get_raw_params()).then([] {
        auto result = ::make_shared<cql_transport::messages::result_message::void_message>();
        make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(result);
    });
}

std::unique_ptr<prepared_statement> alter_session_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<alter_session_statement>(*this));
}

}

}
