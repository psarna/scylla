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

#pragma once

#include <unordered_map>
#include "seastar/core/sstring.hh"
#include "seastar/core/future.hh"
#include "seastar/core/sleep.hh"
#include "seastarx.hh"

namespace debug {

/**
 * Failure injector class can be used to create and manage breakpoints
 * on which a failure can be triggered.
 *
 * Breakpoint registration occurs in code via putting check_breapoint()
 * calls in desired places. check_breakpoint() is purposefully forced inline,
 * so in case this class is disabled statically, it will generate no code,
 * thus preventing unnecessary jumps in fast path.
 * Checking a breakpoint requires a single parameter - breakpoint's name,
 * which can be an arbitrary, human readable string.
 * Some failures may involve overriding the future<> instance in order to inject
 * sleeps or waiting on condition variables, in which case check_breakpoint()
 * should be passed, aside from breakpoint's name, a reference to future<> instance
 * that may be intercepted.
 *
 * Registering a failure can be done either by calling this API directly (e.g. in unit tests)
 * or via REST interface (ref: api/api-doc/failure_injector.json).
 * Registered failure will be triggered once a breakpoint with matching name
 * is checked via check_breakpoint().
 * Each failure registration requires breakpoint name and two control params:
 * - count: the number of times this failure should be retriggered, default: 1
 * - delay: the number of times this failure should not be triggered on a breakpoint check, default: 0
 *
 * Supported types of failures:
 * 1. log_error
 *    Logs an error message passed in argument.
 *    Expected use case: combining with dtests along with its log parsing utilities (wait_for_log and others).
 *
 * 2. throw_exception
 *    Throws an exception - currently narrowed down to std::runtime_error
 *    and exceptions::invalid_request_exception.
 *    Expected use case: mocking a failure in a very specific path - e.g. failing to send a view update
 *    during streaming.
 *
 * 3. abort
 *    Causes abnormal process termination
 *    Expected use case: mocking hardware failure in a very specific path - e.g. crashing the process
 *    in the middle of moving an sstable from /upload dir to data dir.
 *
 * 4. signal
 *    Causes a given signal to be sent to the process
 *    Expected use case: mocking receival of a given signal to be sent in a very specific path - e.g.
 *    receiving SIGTERM in the middle of handling a stream session
 *
 * 5. sleep_for_ms
 *    Sleeps for a given amount of milliseconds. This is seastar::sleep, not a reactor stall.
 *    Requires future<> reference passed to check_breakpoint call.
 *    Expected use case: slowing down the process so it hits external timeouts - e.g. making view update
 *    generation process extremely slow.
 *
 * 6. wait_on_condition
 *    Waits until explicitly woken up from failure injection API - wake_up_from_condition.
 *    Requires future<> reference passed to check_breakpoint call.
 *    Expected use case: making test operations strictly ordered - e.g. making sure that the view building
 *    is not marked finished before we test whether view updates are properly generated during streaming.
 */
class failure_injector {
public:
#ifdef DEBUG_ENABLE_FAILURE_INJECTOR
    static constexpr bool statically_enabled = true;
#else
    static constexpr bool statically_enabled = false;
#endif
    using failure_handler_fun = noncopyable_function<void()>;

    /**
     * Class representing a failure that can be triggered on a breakpoint
     * @param count - How many times this failure should be triggered (default: 1)
     * @param delay - How many times this failure should be skipped
     *                before triggering it (default: 0)
     */
    class failure_handler_base {
    protected:
        unsigned _count;
        unsigned _delay;

        explicit failure_handler_base(unsigned count, unsigned delay)
                : _count(count)
                , _delay(delay)
        {
            assert(count > 0);
        }

    public:
        bool empty() const {
            return _count == 0;
        }
    };

    /**
     * @param _fun - function that will be called when failure is triggered
     */
    struct failure_handler : public failure_handler_base {
        failure_handler_fun _fun;
    public:
        explicit failure_handler(failure_handler_fun&& fun, unsigned count = 1, unsigned delay = 0)
                : failure_handler_base(count, delay)
                , _fun(std::move(fun))
        { }

        void maybe_fail(const sstring& breakpoint_name);
    };

    /**
     * @param _sleep_ms - sleep in milliseconds that should be injected for a future
     *                    passed as a parameter (zero means no sleep is needed)
     */
    struct sleep_handler : public failure_handler_base {
        std::chrono::milliseconds _sleep_ms;
    public:
        explicit sleep_handler(std::chrono::milliseconds sleep_ms, unsigned count = 1, unsigned delay = 0)
            : failure_handler_base(count, delay)
            , _sleep_ms(sleep_ms)
        { }

        std::chrono::milliseconds maybe_need_sleep(const sstring& breakpoint_name);
    };

    struct wait_on_condition_handler : public failure_handler_base {
        condition_variable _cond;
    public:
        explicit wait_on_condition_handler(unsigned count = 1, unsigned delay = 0) : failure_handler_base(count, delay) {}

        condition_variable* maybe_need_wait(const sstring& breakpoint_name);
        void wake_up() {
            _cond.signal();
        }
    };

private:
    std::unordered_map<sstring, failure_handler> _failure_handlers;
    std::unordered_map<sstring, sleep_handler> _sleep_handlers;
    std::unordered_map<sstring, wait_on_condition_handler> _wait_handlers;

public:
    [[gnu::always_inline]]
    void check_breakpoint(const char* breakpoint_name) {
        if constexpr (!statically_enabled) {
            return;
        }
        do_check_breakpoint(breakpoint_name);
    }

    template<typename... Args>
    [[gnu::always_inline]]
    void check_breakpoint(const char* breakpoint_name, future<Args...>& intercepted_future) {
        if constexpr (!statically_enabled) {
            return;
        }

        condition_variable* condition_opt = check_if_needs_wait(breakpoint_name);
        if (condition_opt) {
            intercepted_future = condition_opt->wait().then([intercepted_future = std::move(intercepted_future)] () mutable {
                return std::move(intercepted_future);
            });
        }

        std::chrono::milliseconds needed_sleep = check_if_needs_sleep(breakpoint_name);
        if (needed_sleep != std::chrono::milliseconds::zero()) {
            intercepted_future = seastar::sleep(needed_sleep).then([intercepted_future = std::move(intercepted_future)] () mutable {
                return std::move(intercepted_future);
            });
        }
        do_check_breakpoint(breakpoint_name);
    }

     void register_failure_for(const sstring& breakpoint_name, failure_handler&& handler) {
        if constexpr (!statically_enabled) {
            return;
        }
        do_register_failure_for(breakpoint_name, std::move(handler));
    }

    void register_failure_for(const sstring& breakpoint_name, const sstring& failure_type, const sstring& failure_args, unsigned count = 1, unsigned delay = 0) {
        if constexpr (!statically_enabled) {
            return;
        }
        do_register_failure_for(breakpoint_name, failure_type, failure_args, count, delay);
    }

    void unregister_failure_for(const sstring& breakpoint_name) {
        if constexpr (!statically_enabled) {
            return;
        }
        do_unregister_failure_for(breakpoint_name);
    }

    std::vector<sstring> get_active_breakpoints() const;

private:
    void do_check_breakpoint(const char* breakpoint_name);
    std::chrono::milliseconds check_if_needs_sleep(const char* breakpoint_name);
    condition_variable* check_if_needs_wait(const char* breakpoint_name);
    void do_register_failure_for(const sstring& breakpoint_name, failure_handler&& handler);
    void do_register_failure_for(const sstring& breakpoint_name, const sstring& failure_type, const sstring& failure_args, unsigned count, unsigned delay);
    void do_unregister_failure_for(const sstring& breakpoint_name);
    void maybe_wake_up(const sstring& breakpoint_name);
};

}
