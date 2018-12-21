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

#include "log.hh"
#include "utils/failure_injector.hh"
#include "exceptions/exceptions.hh"
#include <csignal>
#include <thread>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/join.hpp>

namespace utils {

static logging::logger flogger("debug_failure_injector");

std::vector<sstring> failure_injector::get_active_injections() const {
    return boost::copy_range<std::vector<sstring>>(boost::range::join(_failure_handlers | boost::adaptors::map_keys, _sleep_handlers | boost::adaptors::map_keys));
}

void failure_injector::do_check_injection(const char* injection_name) {
    auto it = _failure_handlers.find(sstring(injection_name));
    if (it == _failure_handlers.end()) {
        return;
    }
    failure_handler& handler = it->second;
    if (handler.empty()) {
        _failure_handlers.erase(it);
        return;
    }
    handler.maybe_fail(injection_name);
}

std::chrono::milliseconds failure_injector::check_if_needs_sleep(const char* injection_name) {
    auto it = _sleep_handlers.find(sstring(injection_name));
    if (it == _sleep_handlers.end()) {
        return std::chrono::milliseconds::zero();
    }
    sleep_handler& handler = it->second;
    if (handler.empty()) {
        _sleep_handlers.erase(it);
        return std::chrono::milliseconds::zero();
    }
    return handler.maybe_need_sleep(injection_name);
}

condition_variable* failure_injector::check_if_needs_wait(const char* injection_name) {
    auto it = _wait_handlers.find(sstring(injection_name));
    if (it == _wait_handlers.end()) {
        return nullptr;
    }
    wait_on_condition_handler& handler = it->second;
    if (handler.empty()) {
        _wait_handlers.erase(it);
        return nullptr;
    }
    return handler.maybe_need_wait(injection_name);
}

void failure_injector::do_register_failure_for(const sstring& injection_name, failure_handler&& handler) {
    flogger.debug("Registering failure for injection {}", injection_name);
    _failure_handlers.emplace(injection_name, std::move(handler));
}

static std::unordered_map<sstring, std::function<void(const sstring&)>> predefined_handlers{
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
    {"abort", [] (const sstring&) {
            abort();
        }
    },
    {"signal", [] (const sstring& args) {
            int signum = boost::lexical_cast<int>(args);
            std::raise(signum);
        }
    },
};

void failure_injector::do_register_failure_for(const sstring& injection_name, const sstring& failure_type, const sstring& failure_args, unsigned count, unsigned delay) {
    flogger.debug("Registering failure for injection {}: {} {}", injection_name, failure_type, failure_args);
    if (failure_type == "wait_on_condition") {
        _wait_handlers.emplace(injection_name, wait_on_condition_handler(count, delay));
        return;
    }
    if (failure_type == "wake_up_from_condition") {
        maybe_wake_up(injection_name);
        return;
    }
    if (failure_type == "sleep_for_ms") {
        _sleep_handlers.emplace(injection_name, sleep_handler(std::chrono::milliseconds(boost::lexical_cast<unsigned>(failure_args)), count, delay));
        return;
    }
    auto handler_it = predefined_handlers.find(failure_type);
    if (handler_it == predefined_handlers.end()) {
        throw exceptions::invalid_request_exception("Invalid failure type: " + failure_type);
    }
    do_register_failure_for(injection_name, failure_handler(std::bind(handler_it->second, failure_args), count, delay));
}

void failure_injector::do_unregister_failure_for(const sstring& injection_name) {
    flogger.debug("Unregistering failure for injection {}", injection_name);
    _failure_handlers.erase(injection_name);
}

void failure_injector::maybe_wake_up(const sstring& injection_name) {
    auto it = _wait_handlers.find(injection_name);
    if (it != _wait_handlers.end()) {
        it->second.wake_up();
    }
}

void failure_injector::failure_handler::maybe_fail(const sstring& injection_name) {
    assert(_count > 0);
    if (_delay > 0) {
        --_delay;
        return;
    }
    --_count;
    flogger.debug("Triggering failure for injection {}. Remaining triggers: {}", injection_name, _count);
    _fun();
}

std::chrono::milliseconds failure_injector::sleep_handler::maybe_need_sleep(const sstring& injection_name) {
    assert(_count > 0);
    if (_delay > 0) {
        --_delay;
        return std::chrono::milliseconds::zero();
    }
    --_count;
    flogger.debug("Injecting {}ms delay for injection {}. Remaining triggers: {}", _sleep_ms.count(), injection_name, _count);
    return _sleep_ms;
}

condition_variable* failure_injector::wait_on_condition_handler::maybe_need_wait(const sstring& injection_name) {
    assert(_count > 0);
    if (_delay > 0) {
        --_delay;
        return nullptr;
    }
    --_count;
    flogger.debug("Waiting on condition for injection {}. Remaining triggers: {}", injection_name, _count);
    return std::addressof(_cond);
}

}
