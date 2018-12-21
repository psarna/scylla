/*
 * Copyright (C) 2019 ScyllaDB
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

#include "api/api-doc/failure_injector.json.hh"
#include "api/api.hh"

#include <seastar/http/exception.hh>
#include "log.hh"
#include "utils/failure_injector.hh"
#include "seastar/core/future-util.hh"

namespace api {

namespace hf = httpd::failure_injector_json;

void set_failure_injector(http_context& ctx, routes& r) {
    if (!utils::failure_injector::statically_enabled) {
        throw httpd::bad_param_exception("Failure injection disabled");
    }

    hf::set_injection.set(r, [](std::unique_ptr<request> req) {
        sstring injection = req->param["injection"];
        sstring failure_handler = req->get_query_param("failure_handler");
        sstring failure_args = req->get_query_param("failure_args");

        sstring count_str = req->get_query_param("count");
        sstring delay_str = req->get_query_param("delay");
        unsigned count = count_str.empty() ? 1 : boost::lexical_cast<unsigned>(count_str);
        unsigned delay = delay_str.empty() ? 0 : boost::lexical_cast<unsigned>(delay_str);

        return smp::invoke_on_all([injection, failure_handler, failure_args, count, delay] {
            utils::get_failure_injector().register_failure_for(injection, failure_handler, failure_args, count, delay);
        }).then([] {
            return make_ready_future<json::json_return_type>(json::json_void());
        });

    });

    hf::get_active_injections.set(r, [](std::unique_ptr<request> req) {
        return seastar::map_reduce(smp::all_cpus(), [] (unsigned id) {
            return make_ready_future<std::vector<sstring>>(utils::get_failure_injector().get_active_injections());
        }, std::vector<sstring>(),
            [](std::vector<sstring> a, std::vector<sstring>&& b) mutable -> std::vector<sstring> {
            for (auto&& x : b) {
                if (std::find(a.begin(), a.end(), x) == a.end()) {
                    a.push_back(std::move(x));
                }
            }
            return a;
        }).then([] (std::vector<sstring> ret) {
                return make_ready_future<json::json_return_type>(ret);
        });
    });

    hf::unset_injection.set(r, [](std::unique_ptr<request> req) {
        sstring injection = req->param["injection"];

        return smp::invoke_on_all([injection] {
            utils::get_failure_injector().unregister_failure_for(injection);
        }).then([] {
            return make_ready_future<json::json_return_type>(json::json_void());
        });
    });
}

}
