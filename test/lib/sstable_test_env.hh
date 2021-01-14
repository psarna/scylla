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

#include <seastar/core/do_with.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

#include "sstables/sstables.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/test_services.hh"
#include "test/lib/log.hh"

constexpr auto la = sstables::sstable::version_types::la;
constexpr auto big = sstables::sstable::format_types::big;

class column_family_test {
    lw_shared_ptr<column_family> _cf;
public:
    column_family_test(lw_shared_ptr<column_family> cf) : _cf(cf) {}

    void add_sstable(sstables::shared_sstable sstable) {
        _cf->_sstables->insert(std::move(sstable));
    }

    // NOTE: must run in a thread
    void rebuild_sstable_list(const std::vector<sstables::shared_sstable>& new_sstables,
            const std::vector<sstables::shared_sstable>& sstables_to_remove) {
        _cf->_sstables = _cf->build_new_sstable_list(new_sstables, sstables_to_remove).get0();
    }

    static void update_sstables_known_generation(column_family& cf, unsigned generation) {
        cf.update_sstables_known_generation(generation);
    }

    static uint64_t calculate_generation_for_new_table(column_family& cf) {
        return cf.calculate_generation_for_new_table();
    }

    static int64_t calculate_shard_from_sstable_generation(int64_t generation) {
        return column_family::calculate_shard_from_sstable_generation(generation);
    }

    future<stop_iteration> try_flush_memtable_to_sstable(lw_shared_ptr<memtable> mt) {
        return _cf->try_flush_memtable_to_sstable(mt, sstable_write_permit::unconditional());
    }
};

namespace sstables {

class test_env {
    std::unique_ptr<sstables_manager> _mgr;
public:
    explicit test_env() : _mgr(std::make_unique<sstables_manager>(nop_lp_handler, test_db_config, test_feature_service)) { }

    future<> stop() {
        return _mgr->close();
    }

    shared_sstable make_sstable(schema_ptr schema, sstring dir, unsigned long generation,
            sstable::version_types v, sstable::format_types f = sstable::format_types::big,
            size_t buffer_size = default_sstable_buffer_size, gc_clock::time_point now = gc_clock::now()) {
        return _mgr->make_sstable(std::move(schema), dir, generation, v, f, now, default_io_error_handler_gen(), buffer_size);
    }

    future<shared_sstable> reusable_sst(schema_ptr schema, sstring dir, unsigned long generation,
            sstable::version_types version = sstable::version_types::la, sstable::format_types f = sstable::format_types::big) {
        auto sst = make_sstable(std::move(schema), dir, generation, version, f);
        return sst->load().then([sst = std::move(sst)] {
            return make_ready_future<shared_sstable>(std::move(sst));
        });
    }

    sstables_manager& manager() { return *_mgr; }

    future<> working_sst(schema_ptr schema, sstring dir, unsigned long generation) {
        return reusable_sst(std::move(schema), dir, generation).then([] (auto ptr) { return make_ready_future<>(); });
    }

    template <typename Func>
    static inline auto do_with(Func&& func) {
        return seastar::do_with(test_env(), [func = std::move(func)] (test_env& env) mutable {
            return func(env).finally([&env] {
                return env.stop();
            });
        });
    }

    template <typename T, typename Func>
    static inline auto do_with(T&& rval, Func&& func) {
        return seastar::do_with(test_env(), std::forward<T>(rval), [func = std::move(func)] (test_env& env, T& val) mutable {
            return func(env, val).finally([&env] {
                return env.stop();
            });
        });
    }

    static inline future<> do_with_async(noncopyable_function<void (test_env&)> func) {
        return seastar::async([func = std::move(func)] {
            test_env env;
            auto close_env = defer([&] { env.stop().get(); });
            func(env);
        });
    }

    static inline future<> do_with_sharded_async(noncopyable_function<void (sharded<test_env>&)> func) {
        return seastar::async([func = std::move(func)] {
            sharded<test_env> env;
            env.start().get();
            auto stop = defer([&] { env.stop().get(); });
            func(env);
        });
    }

    template <typename T>
    static future<T> do_with_async_returning(noncopyable_function<T (test_env&)> func) {
        return seastar::async([func = std::move(func)] {
            test_env env;
            auto stop = defer([&] { env.stop().get(); });
            return func(env);
        });
    }
};

}   // namespace sstables
