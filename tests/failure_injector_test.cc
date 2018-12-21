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


#include "tests/test-utils.hh"

#include "utils/failure_injector.hh"
#include "log.hh"

using namespace std::literals::chrono_literals;

static logging::logger flogger("failure_injection_test");

BOOST_AUTO_TEST_CASE(test_simple_inject) {
    utils::failure_injector injector;
    if (!utils::failure_injector::statically_enabled) {
        injector.register_failure_for("wrong", "throw_exception", "runtime_error", 1, 0);
        injector.check_injection("wrong");
        return;
    }

    injector.register_failure_for("bp1", "throw_exception", "runtime_error", 1, 0);

    injector.check_injection("bp2");
    BOOST_REQUIRE_THROW(injector.check_injection("bp1"), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(test_inject_with_delay_and_count) {
    if (!utils::failure_injector::statically_enabled) {
        flogger.warn("Test case statically disabled.");
        return;
    }

    utils::failure_injector injector;

    utils::failure_injector::failure_handler h([] { throw std::runtime_error("failed"); }, 5, 3);
    injector.register_failure_for("bp1", std::move(h));

    for (int i = 0; i < 3; ++i) {
        injector.check_injection("bp1");
    }
    for (int i = 3; i < 8; ++i) {
        BOOST_REQUIRE_THROW(injector.check_injection("bp1"), std::runtime_error);
    }
    for (int i = 8; i < 100; ++i) {
        injector.check_injection("bp1");
    }
}

BOOST_AUTO_TEST_CASE(test_cancel) {
    if (!utils::failure_injector::statically_enabled) {
        flogger.warn("Test case statically disabled.");
        return;
    }

    utils::failure_injector injector;

    injector.register_failure_for("bp1", "throw_exception", "runtime_error", 1, 0);
    injector.register_failure_for("bp2", "throw_exception", "runtime_error", 1, 0);
    injector.unregister_failure_for("bp1");

    injector.check_injection("bp1");
    BOOST_REQUIRE_THROW(injector.check_injection("bp2"), std::runtime_error);
}

SEASTAR_TEST_CASE(test_sleep) {
    if (!utils::failure_injector::statically_enabled) {
        flogger.warn("Test case statically disabled.");
        return make_ready_future<>();
    }

    utils::failure_injector injector;
    injector.register_failure_for("bp1", "sleep_for_ms", "2000", 1, 0);

    auto f = make_ready_future<>();
    injector.check_injection("bp1", f);
    auto start_point = std::chrono::steady_clock::now();
    return f.then([start_point] {
        auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_point);
        BOOST_REQUIRE_GE(wait_time.count(), 2000);
    });
}

SEASTAR_TEST_CASE(test_wait_on_condition) {
    if (!utils::failure_injector::statically_enabled) {
        flogger.warn("Test case statically disabled.");
        return make_ready_future<>();
    }

    static utils::failure_injector injector;
    injector.register_failure_for("bp1", "wait_on_condition", "", 1, 0);

    auto f = make_ready_future<>();
    injector.check_injection("bp1", f);
    auto start_point = std::chrono::steady_clock::now();
    // Fire off a continuation without waiting for it
    seastar::sleep(2000ms).then([] {
            injector.register_failure_for("bp1", "wake_up_from_condition", "", 0, 0);
    });
    return f.then([start_point] {
        auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_point);
        BOOST_REQUIRE_GE(wait_time.count(), 2000);
    });
}
