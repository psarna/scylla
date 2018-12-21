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

namespace utils {

/**
 * Failure injector class can be used to create and manage injection points
 * on which a failure can be triggered.
 *
 * Injection point registration occurs in code via putting check_injection()
 * calls in desired places. check_injection() is purposefully forced inline,
 * so in case this class is disabled statically, it will generate no code,
 * thus preventing unnecessary jumps in fast path.
 * Checking an injection requires a single parameter - injection's name,
 * which can be an arbitrary, human readable string.
 * Some failures may involve overriding the future<> instance in order to inject
 * sleeps or waiting on condition variables, in which case check_injection()
 * should be passed, aside from injection's name, a reference to future<> instance
 * that may be intercepted.
 *
 * Registering a failure can be done either by calling this API directly (e.g. in unit tests)
 * or via REST interface (ref: api/api-doc/failure_injector.json).
 * Registered failure will be triggered once an injection with matching name
 * is checked via check_injection().
 * Each failure registration requires injection name and two control params:
 * - count: the number of times this failure should be retriggered, default: 1
 * - delay: the number of times this failure should not be triggered on an injection check, default: 0
 *
 * Supported types of failures:
 * 1. throw_exception
 *    Throws an exception - currently narrowed down to std::runtime_error
 *    and exceptions::invalid_request_exception.
 *    Expected use case: mocking a failure in a very specific path - e.g. failing to send a view update
 *    during streaming.
 *
 * 2. abort
 *    Causes abnormal process termination
 *    Expected use case: mocking hardware failure in a very specific path - e.g. crashing the process
 *    in the middle of moving an sstable from /upload dir to data dir.
 *
 * 3. signal
 *    Causes a given signal to be sent to the process
 *    Expected use case: mocking receival of a given signal to be sent in a very specific path - e.g.
 *    receiving SIGTERM in the middle of handling a stream session
 *
 * 4. sleep_for_ms
 *    Sleeps for a given amount of milliseconds. This is seastar::sleep, not a reactor stall.
 *    Requires future<> reference passed to check_injection call.
 *    Expected use case: slowing down the process so it hits external timeouts - e.g. making view update
 *    generation process extremely slow.
 *
 * 5. wait_on_condition
 *    Waits until explicitly woken up from failure injection API - wake_up_from_condition.
 *    Requires future<> reference passed to check_injection call.
 *    Expected use case: making test operations strictly ordered - e.g. making sure that the view building
 *    is not marked finished before we test whether view updates are properly generated during streaming.
 *
 * 6. custom injection
 *    An injection can also accept custom lambda function that will run once triggered.
 *    It can be achieved by running register_failure_for(injection_name, failure_handler) constructor.
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
     * Class representing a failure that can be triggered on an injection
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

        virtual ~failure_handler_base() {}

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

        void maybe_fail(const sstring& injection_name);
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

        std::chrono::milliseconds maybe_need_sleep(const sstring& injection_name);
    };

    struct wait_on_condition_handler : public failure_handler_base {
        condition_variable _cond;
    public:
        explicit wait_on_condition_handler(unsigned count = 1, unsigned delay = 0) : failure_handler_base(count, delay) {}

        condition_variable* maybe_need_wait(const sstring& injection_name);
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
    void check_injection(const char* injection_name) {
        if constexpr (!statically_enabled) {
            return;
        }
        do_check_injection(injection_name);
    }

    template<typename... Args>
    [[gnu::always_inline]]
    void check_injection(const char* injection_name, future<Args...>& intercepted_future) {
        if constexpr (!statically_enabled) {
            return;
        }

        condition_variable* condition_opt = check_if_needs_wait(injection_name);
        if (condition_opt) {
            intercepted_future = condition_opt->wait().then([intercepted_future = std::move(intercepted_future)] () mutable {
                return std::move(intercepted_future);
            });
        }

        std::chrono::milliseconds needed_sleep = check_if_needs_sleep(injection_name);
        if (needed_sleep != std::chrono::milliseconds::zero()) {
            intercepted_future = seastar::sleep(needed_sleep).then([intercepted_future = std::move(intercepted_future)] () mutable {
                return std::move(intercepted_future);
            });
        }
        do_check_injection(injection_name);
    }

     void register_failure_for(const sstring& injection_name, failure_handler&& handler) {
        if constexpr (!statically_enabled) {
            return;
        }
        do_register_failure_for(injection_name, std::move(handler));
    }

    void register_failure_for(const sstring& injection_name, const sstring& failure_type, const sstring& failure_args, unsigned count = 1, unsigned delay = 0) {
        if constexpr (!statically_enabled) {
            return;
        }
        do_register_failure_for(injection_name, failure_type, failure_args, count, delay);
    }

    void unregister_failure_for(const sstring& injection_name) {
        if constexpr (!statically_enabled) {
            return;
        }
        do_unregister_failure_for(injection_name);
    }

    std::vector<sstring> get_active_injections() const;

private:
    void do_check_injection(const char* injection_name);
    std::chrono::milliseconds check_if_needs_sleep(const char* injection_name);
    condition_variable* check_if_needs_wait(const char* injection_name);
    void do_register_failure_for(const sstring& injection_name, failure_handler&& handler);
    void do_register_failure_for(const sstring& injection_name, const sstring& failure_type, const sstring& failure_args, unsigned count, unsigned delay);
    void do_unregister_failure_for(const sstring& injection_name);
    void maybe_wake_up(const sstring& injection_name);
};

}
