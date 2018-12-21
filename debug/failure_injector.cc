/*
 * Copyright (C) 2018 ScyllaDB
 *
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

#include "log.hh"
#include "debug/failure_injector.hh"
#include "exceptions/exceptions.hh"
#include <thread>
#include <boost/range/adaptor/map.hpp>

namespace debug {

static logging::logger flogger("debug_failure_injector");

std::vector<sstring> failure_injector::get_active_breakpoints() const {
    return boost::copy_range<std::vector<sstring>>(_failure_handlers | boost::adaptors::map_keys);
}

void failure_injector::do_check_breakpoint(const sstring& breakpoint_name) {
    auto it = _failure_handlers.find(breakpoint_name);
    if (it == _failure_handlers.end()) {
        return;
    }
    failure_handler& handler = it->second;
    if (handler.empty()) {
        _failure_handlers.erase(it);
        return;
    }
    handler.maybe_fail(breakpoint_name);
}

void failure_injector::do_register_failure_for(const sstring& breakpoint_name, failure_handler&& handler) {
    flogger.debug("Registering failure for breakpoint {}", breakpoint_name);
    _failure_handlers.emplace(breakpoint_name, std::move(handler));
}

static std::unordered_map<sstring, std::function<void(const sstring&)>> predefined_handlers {
    {"log_error", [] (const sstring& args) {
           flogger.error("{}", args);
        }
    },
    {"throw_exception", [] (const sstring& args) {
            if (args == "runtime_error") {
                throw std::runtime_error("injected");
            } else if (args == "invalid_request_exception") {
                throw exceptions::invalid_request_exception("injected");
            } else {
                throw std::out_of_range(std::string(args));
            }
        }
    },
    {"stall", [] (const sstring& args) {
            try {
                unsigned stall_in_ms = boost::lexical_cast<unsigned>(std::string(args));
                std::this_thread::sleep_for(std::chrono::milliseconds(stall_in_ms));
            } catch (boost::bad_lexical_cast& e) {
                throw std::runtime_error("Invalid milliseconds value " + args);
            }
        }
    },
};

void failure_injector::do_register_failure_for(const sstring& breakpoint_name, const sstring& failure_type, const sstring& failure_args, unsigned count, unsigned delay) {
    flogger.debug("Registering failure for breakpoint {}: {} {}", breakpoint_name, failure_type, failure_args);

    do_register_failure_for(breakpoint_name, failure_handler(std::bind(predefined_handlers.at(failure_type), failure_args), count, delay));
}

void failure_injector::do_unregister_failure_for(const sstring& breakpoint_name) {
    flogger.debug("Unregistering failure for breakpoint {}", breakpoint_name);
    _failure_handlers.erase(breakpoint_name);
}

void failure_injector::failure_handler::maybe_fail(const sstring& breakpoint_name) {
    assert(_count > 0);
    if (_delay > 0) {
        --_delay;
        return;
    }
    --_count;
    flogger.debug("Triggering failure for breakpoint {}. Remaining triggers: {}", breakpoint_name, _count);
    _fun();
}

}
