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

#pragma once

#include <unordered_map>
#include "seastar/core/sstring.hh"
#include "seastarx.hh"

namespace debug {

class failure_injector {
    static constexpr bool statically_enabled = true;

public:
    using failure_handler_fun = std::function<void()>;
    class failure_handler {
        unsigned _count;
        unsigned _delay;
        failure_handler_fun _fun;

    public:
        failure_handler(failure_handler_fun&& fun, unsigned count = 1, unsigned delay = 0)
                : _count(count)
                , _delay(delay)
                , _fun(std::move(fun))
        {
            assert(count > 0);
        }

        failure_handler(failure_handler&& other)
                : _count(other._count)
                , _delay(other._delay)
                , _fun(std::move(other._fun))
        { }

        failure_handler& operator=(failure_handler&& other) {
            if (this == std::addressof(other)) {
                return *this;
            }
            _count = other._count;
            _delay = other._delay;
            _fun = std::move(_fun);
            return *this;
        }

        bool empty() const {
            return _count == 0 || !_fun;
        }

        void maybe_fail(const sstring& breakpoint_name);
    };

private:
    bool _enabled;
    std::unordered_map<sstring, failure_handler> _failure_handlers;

public:

    failure_injector(bool enabled) : _enabled(enabled) { }

    [[gnu::always_inline]]
    void check_breakpoint(const sstring& breakpoint_name) {
        if constexpr (!statically_enabled) {
            return;
        }
        if (!_enabled) {
            return;
        }
        do_check_breakpoint(breakpoint_name);
    }

    [[gnu::always_inline]]
     void register_failure_for(const sstring& breakpoint_name, failure_handler&& handler) {
        if constexpr (!statically_enabled) {
            return;
        }
        if (!_enabled) {
            return;
        }
        do_register_failure_for(breakpoint_name, std::move(handler));
    }

    [[gnu::always_inline]]
    void register_failure_for(const sstring& breakpoint_name, const sstring& failure_type, const sstring& failure_args, unsigned count = 1, unsigned delay = 0) {
        if constexpr (!statically_enabled) {
            return;
        }
        if (!_enabled) {
            return;
        }
        do_register_failure_for(breakpoint_name, failure_type, failure_args, count, delay);
    }

    [[gnu::always_inline]]
    void unregister_failure_for(const sstring& breakpoint_name) {
        if constexpr (!statically_enabled) {
            return;
        }
        if (!_enabled) {
            return;
        }
        do_unregister_failure_for(breakpoint_name);
    }

    std::vector<sstring> get_active_breakpoints() const;

private:
    void do_check_breakpoint(const sstring& breakpoint_name);
    void do_register_failure_for(const sstring& breakpoint_name, failure_handler&& handler);
    void do_register_failure_for(const sstring& breakpoint_name, const sstring& failure_type, const sstring& failure_args, unsigned count, unsigned delay);
    void do_unregister_failure_for(const sstring& breakpoint_name);
};

}
