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

#include "cql3/statements/alter_session_statement.hh"

#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

alter_session_statement::alter_session_statement(::shared_ptr<alter_session_prop_defs> props)
        : cql_statement_no_metadata(&timeout_config::other_timeout)
        , _props(props)
        , _to_delete{}
{
}

alter_session_statement::alter_session_statement(sstring to_delete)
        : cql_statement_no_metadata(&timeout_config::other_timeout)
        , _props{}
        , _to_delete(std::move(to_delete))
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
    if (_props) {
        _props->validate();
    }
}

future<::shared_ptr<cql_transport::messages::result_message>>
alter_session_statement::execute(service::storage_proxy& proxy, service::query_state& query_state, const query_options& options) const {
    service::client_state& state = query_state.get_client_state();
    std::map<sstring, sstring> raw_params_map;
    raw_params_map = state.get_session_params().to_map();
    if (_to_delete) {
        assert(!_props);
        raw_params_map.erase(*_to_delete);
    } else {
        for (auto&& new_param : _props->get_raw_params()) {
            raw_params_map[std::move(new_param.first)] = std::move(new_param.second);
        }
    }
    state.set_session_params(alter_session_prop_defs::get_params(raw_params_map));
    auto result = ::make_shared<cql_transport::messages::result_message::void_message>();
    return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(result);
}

std::unique_ptr<prepared_statement> alter_session_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<alter_session_statement>(*this));
}

}

}
