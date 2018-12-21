/*
 * Copyright (C) 2018 ScyllaDB
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

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "debug/failure_injector.hh"

BOOST_AUTO_TEST_CASE(test_simple_inject) {
    debug::failure_injector active_injector(true);
    debug::failure_injector inactive_injector(false);

    active_injector.register_failure_for("bp1", "throw_exception", "runtime_error", 1, 0);
    inactive_injector.register_failure_for("bp1", "throw_exception", "runtime_error", 1, 0);

    inactive_injector.check_breakpoint("bp1");
    active_injector.check_breakpoint("bp2");
    BOOST_REQUIRE_THROW(active_injector.check_breakpoint("bp1"), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(test_inject_with_delay_and_count) {
    debug::failure_injector injector(true);

    debug::failure_injector::failure_handler h([] { throw std::runtime_error("failed"); }, 5, 3);
    injector.register_failure_for("bp1", std::move(h));

    for (int i = 0; i < 3; ++i) {
        injector.check_breakpoint("bp1");
    }
    for (int i = 3; i < 8; ++i) {
        BOOST_REQUIRE_THROW(injector.check_breakpoint("bp1"), std::runtime_error);
    }
    for (int i = 8; i < 100; ++i) {
        injector.check_breakpoint("bp1");
    }
}

BOOST_AUTO_TEST_CASE(test_cancel) {
    debug::failure_injector active_injector(true);

    active_injector.register_failure_for("bp1", "throw_exception", "runtime_error", 1, 0);
    active_injector.register_failure_for("bp2", "throw_exception", "runtime_error", 1, 0);
    active_injector.unregister_failure_for("bp1");

    active_injector.check_breakpoint("bp1");
    BOOST_REQUIRE_THROW(active_injector.check_breakpoint("bp2"), std::runtime_error);
}
