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
 * Copyright (C) 2016 ScyllaDB
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

#include "client_state.hh"
#include "auth/authorizer.hh"
#include "auth/authenticator.hh"
#include "auth/common.hh"
#include "auth/resource.hh"
#include "exceptions/exceptions.hh"
#include "validation.hh"
#include "db/system_keyspace.hh"
#include "db/schema_tables.hh"
#include "tracing/trace_keyspace_helper.hh"
#include "storage_service.hh"
#include "db/system_distributed_keyspace.hh"
#include "database.hh"
#include "cdc/log.hh"
#include "concrete_types.hh"

thread_local api::timestamp_type service::client_state::_last_timestamp_micros = 0;

void service::client_state::set_login(auth::authenticated_user user) {
    _user = std::move(user);
}

service::client_state::client_state(external_tag, auth::service& auth_service, timeout_config timeout_config, const socket_address& remote_address, bool thrift)
        : _is_internal(false)
        , _is_thrift(thrift)
        , _remote_address(remote_address)
        , _auth_service(&auth_service)
        , _default_timeout_config(timeout_config)
        , _timeout_config(timeout_config) {
    if (!auth_service.underlying_authenticator().require_authentication()) {
        _user = auth::authenticated_user();
    }
}

future<> service::client_state::update_per_role_params() {
    if (!_user || auth::is_anonymous(*_user)) {
        return make_ready_future<>();
    }
    //FIXME: replace with a coroutine once they're widely accepted
    return seastar::async([this] {
        auth::role_set roles = _auth_service->get_roles(*_user->name).get();
        db::timeout_clock::duration read_timeout = db::timeout_clock::duration::max();
        db::timeout_clock::duration write_timeout = db::timeout_clock::duration::max();

        auto get_duration = [&] (const sstring& repr) {
            data_value v = duration_type->deserialize(duration_type->from_string(repr));
            cql_duration duration = static_pointer_cast<const duration_type_impl>(duration_type)->from_value(v);
            return std::chrono::duration_cast<lowres_clock::duration>(std::chrono::nanoseconds(duration.nanoseconds));
        };

        for (const auto& role : roles) {
            auto options = _auth_service->underlying_role_manager().query_custom_options(role).get();
            auto read_timeout_it = options.find("read_timeout");
            if (read_timeout_it != options.end()) {
                read_timeout = std::min(read_timeout, get_duration(read_timeout_it->second));
            }
            auto write_timeout_it = options.find("write_timeout");
            if (write_timeout_it != options.end()) {
                write_timeout = std::min(write_timeout, get_duration(write_timeout_it->second));
            }
         }
        _timeout_config.read_timeout = read_timeout == db::timeout_clock::duration::max() ?
                _default_timeout_config.read_timeout : read_timeout;
        _timeout_config.range_read_timeout = read_timeout == db::timeout_clock::duration::max() ?
                _default_timeout_config.range_read_timeout : read_timeout;
        _timeout_config.write_timeout = write_timeout == db::timeout_clock::duration::max() ?
                _default_timeout_config.write_timeout : write_timeout;
        _timeout_config.counter_write_timeout = write_timeout == db::timeout_clock::duration::max() ?
                _default_timeout_config.counter_write_timeout : write_timeout;
    });
}

future<> service::client_state::check_user_can_login() {
    if (auth::is_anonymous(*_user)) {
        return make_ready_future();
    }

    const auto& role_manager = _auth_service->underlying_role_manager();

    return role_manager.exists(*_user->name).then([this](bool exists) mutable {
        if (!exists) {
            throw exceptions::authentication_exception(
                            format("User {} doesn't exist - create it with CREATE USER query first",
                                            *_user->name));
        }
        return make_ready_future();
    }).then([this, &role_manager] {
        return role_manager.can_login(*_user->name).then([this](bool can_login) {
            if (!can_login) {
                throw exceptions::authentication_exception(format("{} is not permitted to log in", *_user->name));
            }

            return make_ready_future();
        });
    });
}

void service::client_state::validate_login() const {
    if (!_user) {
        throw exceptions::unauthorized_exception("You have not logged in");
    }
}

void service::client_state::ensure_not_anonymous() const {
    validate_login();
    if (auth::is_anonymous(*_user)) {
        throw exceptions::unauthorized_exception("You have to be logged in and not anonymous to perform this request");
    }
}

future<> service::client_state::has_all_keyspaces_access(
                auth::permission p) const {
    if (_is_internal) {
        return make_ready_future();
    }
    validate_login();

    return do_with(auth::resource(auth::resource_kind::data), [this, p](const auto& r) {
        return ensure_has_permission({p, r});
    });
}

future<> service::client_state::has_keyspace_access(const sstring& ks,
                auth::permission p) const {
    return do_with(ks, auth::make_data_resource(ks), [this, p](auto const& ks, auto const& r) {
        return has_access(ks, {p, r});
    });
}

future<> service::client_state::has_column_family_access(const sstring& ks,
                const sstring& cf, auth::permission p, auth::command_desc::type t) const {
    validation::validate_column_family(ks, cf);

    return do_with(ks, auth::make_data_resource(ks, cf), [this, p, t](const auto& ks, const auto& r) {
        return has_access(ks, {p, r, t});
    });
}

future<> service::client_state::has_schema_access(const schema& s, auth::permission p) const {
    return do_with(
            s.ks_name(),
            auth::make_data_resource(s.ks_name(),s.cf_name()),
            [this, p](auto const& ks, auto const& r) {
        return has_access(ks, {p, r});
    });
}

std::map<sstring, sstring> service::client_state::per_session_params_map() const {
    std::map<sstring, sstring> map;
    auto to_duration_string = [] (int64_t nanos) {
        if (nanos == 0) {
            return sstring("0s");
        }
        return to_string(cql_duration(months_counter(0), days_counter(0), nanoseconds_counter(nanos)));
    };
    int64_t read_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(_timeout_config.read_timeout).count();
    map.emplace("read_timeout", to_duration_string(read_nanos));

    int64_t write_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(_timeout_config.write_timeout).count();
    map.emplace("write_timeout", to_duration_string(write_nanos));

    return map;
}

future<> service::client_state::has_access(const sstring& ks, auth::command_desc cmd) const {
    if (ks.empty()) {
        throw exceptions::invalid_request_exception("You have not set a keyspace for this session");
    }
    if (_is_internal) {
        return make_ready_future();
    }

    validate_login();

    static const auto alteration_permissions = auth::permission_set::of<
            auth::permission::CREATE, auth::permission::ALTER, auth::permission::DROP>();

    // we only care about schema modification.
    if (alteration_permissions.contains(cmd.permission)) {
        // prevent system keyspace modification
        auto name = ks;
        std::transform(name.begin(), name.end(), name.begin(), ::tolower);
        if (is_system_keyspace(name)) {
            throw exceptions::unauthorized_exception(ks + " keyspace is not user-modifiable.");
        }

        //
        // we want to disallow dropping any contents of TRACING_KS and disallow dropping the `auth::meta::AUTH_KS`
        // keyspace.
        //

        const bool dropping_anything_in_tracing = (name == tracing::trace_keyspace_helper::KEYSPACE_NAME)
                && (cmd.permission == auth::permission::DROP);

        const bool dropping_auth_keyspace = (cmd.resource == auth::make_data_resource(auth::meta::AUTH_KS))
                && (cmd.permission == auth::permission::DROP);

        if (dropping_anything_in_tracing || dropping_auth_keyspace) {
            throw exceptions::unauthorized_exception(
                    format("Cannot {} {}", auth::permissions::to_string(cmd.permission), cmd.resource));
        }
    }

    static thread_local std::unordered_set<auth::resource> readable_system_resources = [] {
        std::unordered_set<auth::resource> tmp;
        for (auto cf : { db::system_keyspace::LOCAL, db::system_keyspace::PEERS }) {
            tmp.insert(auth::make_data_resource(db::system_keyspace::NAME, cf));
        }
        for (auto cf : db::schema_tables::all_table_names(db::schema_features::full())) {
            tmp.insert(auth::make_data_resource(db::schema_tables::NAME, cf));
        }
        return tmp;
    }();

    if (cmd.permission == auth::permission::SELECT && readable_system_resources.contains(cmd.resource)) {
        return make_ready_future();
    }
    if (alteration_permissions.contains(cmd.permission)) {
        if (auth::is_protected(*_auth_service, cmd)) {
            throw exceptions::unauthorized_exception(format("{} is protected", cmd.resource));
        }
    }

    if (service::get_local_storage_service().db().local().features().cluster_supports_cdc()
        && cmd.resource.kind() == auth::resource_kind::data) {
        const auto resource_view = auth::data_resource_view(cmd.resource);
        if (resource_view.table()) {
            if (cmd.permission == auth::permission::DROP) {
                if (cdc::is_log_for_some_table(ks, *resource_view.table())) {
                    throw exceptions::unauthorized_exception(
                            format("Cannot {} cdc log table {}", auth::permissions::to_string(cmd.permission), cmd.resource));
                }
            }

            static constexpr auto cdc_topology_description_forbidden_permissions = auth::permission_set::of<
                    auth::permission::ALTER, auth::permission::DROP>();

            if (cdc_topology_description_forbidden_permissions.contains(cmd.permission)) {
                if (ks == db::system_distributed_keyspace::NAME
                        && (resource_view.table() == db::system_distributed_keyspace::CDC_DESC
                        || resource_view.table() == db::system_distributed_keyspace::CDC_TOPOLOGY_DESCRIPTION)) {
                    throw exceptions::unauthorized_exception(
                            format("Cannot {} {}", auth::permissions::to_string(cmd.permission), cmd.resource));
                }
            }
        }
    }

    return ensure_has_permission(cmd);
}

future<bool> service::client_state::check_has_permission(auth::command_desc cmd) const {
    if (_is_internal) {
        return make_ready_future<bool>(true);
    }

    return do_with(cmd.resource.parent(), [this, cmd](const std::optional<auth::resource>& parent_r) {
        return auth::get_permissions(*_auth_service, *_user, cmd.resource).then(
                [this, p = cmd.permission, &parent_r](auth::permission_set set) {
            if (set.contains(p)) {
                return make_ready_future<bool>(true);
            }
            if (parent_r) {
                return check_has_permission({p, *parent_r});
            }
            return make_ready_future<bool>(false);
        });
    });
}

future<> service::client_state::ensure_has_permission(auth::command_desc cmd) const {
    return check_has_permission(cmd).then([this, cmd](bool ok) {
        if (!ok) {
            throw exceptions::unauthorized_exception(
                format("User {} has no {} permission on {} or any of its parents",
                        *_user,
                        auth::permissions::to_string(cmd.permission),
                        cmd.resource));
        }
    });
}

void service::client_state::set_keyspace(database& db, std::string_view keyspace) {
    // Skip keyspace validation for non-authenticated users. Apparently, some client libraries
    // call set_keyspace() before calling login(), and we have to handle that.
    if (_user && !db.has_keyspace(keyspace)) {
        throw exceptions::invalid_request_exception(format("Keyspace '{}' does not exist", keyspace));
    }
    _keyspace = sstring(keyspace);
}

future<> service::client_state::ensure_exists(const auth::resource& r) const {
    return _auth_service->exists(r).then([&r](bool exists) {
        if (!exists) {
            throw exceptions::invalid_request_exception(format("{} doesn't exist.", r));
        }

        return make_ready_future<>();
    });
}
