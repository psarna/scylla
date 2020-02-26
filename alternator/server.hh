/*
 * Copyright 2019 ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "alternator/executor.hh"
#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/tls.hh>
#include <optional>
#include <alternator/auth.hh>
#include <utils/small_vector.hh>
#include <seastar/core/units.hh>

namespace alternator {

class server {
    static constexpr size_t content_length_limit = 16*MB;
    using alternator_callback = std::function<future<executor::request_return_type>(executor&, executor::client_state&, tracing::trace_state_ptr, rjson::value, std::unique_ptr<request>)>;
    using alternator_callbacks_map = std::unordered_map<std::string_view, alternator_callback>;

    seastar::httpd::http_server_control _control;
    seastar::httpd::http_server_control _https_control;
    seastar::sharded<executor>& _executor;
    key_cache _key_cache;
    bool _enforce_authorization;
    utils::small_vector<std::reference_wrapper<seastar::httpd::http_server_control>, 2> _enabled_servers;
    seastar::sharded<seastar::gate> _pending_requests;
    alternator_callbacks_map _callbacks;
public:
    server(seastar::sharded<executor>& executor);

    seastar::future<> init(net::inet_address addr, std::optional<uint16_t> port, std::optional<uint16_t> https_port, std::optional<tls::credentials_builder> creds, bool enforce_authorization);
    future<> stop();
private:
    void set_routes(seastar::httpd::routes& r);
    future<> verify_signature(const seastar::httpd::request& r);
    future<executor::request_return_type> handle_api_request(std::unique_ptr<request>&& req);
};

}

