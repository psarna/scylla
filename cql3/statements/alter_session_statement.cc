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
#include "db/system_keyspace.hh"
#include "db/query_context.hh"
#include "types.hh"
#include "concrete_types.hh"
#include "types/map.hh"
#include "connection_notifier.hh"

#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

alter_session_statement::alter_session_statement(sstring key , sstring value)
        : cql_statement_no_metadata(&timeout_config::other_timeout)
        , _key(key)
        , _value(value)
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
}

static future<> update_system_clients_table(service::client_state& state) {
    const static sstring req = format("UPDATE system.{} SET params = ? WHERE address = ? AND port = ? AND client_type = '{}';",
            db::system_keyspace::CLIENTS, to_string(client_type::cql));
    std::vector<std::pair<data_value, data_value>> entries;
    for (auto&& entry : state.get_session_params().to_map()) {
        entries.push_back({data_value(entry.first), data_value(entry.second)});
    }

    return db::qctx->execute_cql(req,
            make_map_value(map_type_impl::get_instance(utf8_type, utf8_type, false), entries),
            state.get_client_address(),
            state.get_client_port())
        .discard_result();
}

static service::client_state::session_params
get_params(const std::map<sstring, sstring>& raw_params) {
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

future<::shared_ptr<cql_transport::messages::result_message>>
alter_session_statement::execute(service::storage_proxy& proxy, service::query_state& query_state, const query_options& options) const {
    service::client_state& state = query_state.get_client_state();
    std::map<sstring, sstring> raw_params_map;
    raw_params_map = state.get_session_params().to_map();
    if (_value == "null") {
        raw_params_map.erase(_key);
    } else {
        raw_params_map[_key] = _value;
    }
    state.set_session_params(get_params(raw_params_map));
    return update_system_clients_table(state).then([] {
        auto result = ::make_shared<cql_transport::messages::result_message::void_message>();
        return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(result);
    });
}

std::unique_ptr<prepared_statement> alter_session_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<alter_session_statement>(*this));
}

}

}
