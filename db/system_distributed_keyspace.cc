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

#include "db/system_distributed_keyspace.hh"

#include "cql3/untyped_result_set.hh"
#include "database.hh"
#include "db/consistency_level_type.hh"
#include "db/system_keyspace.hh"
#include "schema_builder.hh"
#include "timeout_config.hh"
#include "types.hh"
#include "types/tuple.hh"
#include "types/set.hh"
#include "cdc/generation.hh"
#include "cql3/query_processor.hh"

#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/coroutine.hh>

#include <boost/range/adaptor/transformed.hpp>

#include <optional>
#include <vector>
#include <optional>

static logging::logger dlogger("system_distributed_keyspace");
extern logging::logger cdc_log;

namespace db {

thread_local data_type cdc_streams_set_type = set_type_impl::get_instance(bytes_type, false);

/* See `token_range_description` struct */
thread_local data_type cdc_streams_list_type = list_type_impl::get_instance(bytes_type, false);
thread_local data_type cdc_token_range_description_type = tuple_type_impl::get_instance(
        { long_type             // dht::token token_range_end;
        , cdc_streams_list_type // std::vector<stream_id> streams;
        , byte_type             // uint8_t sharding_ignore_msb;
        });
thread_local data_type cdc_generation_description_type = list_type_impl::get_instance(cdc_token_range_description_type, false);

schema_ptr view_build_status() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS);
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS, std::make_optional(id))
                .with_column("keyspace_name", utf8_type, column_kind::partition_key)
                .with_column("view_name", utf8_type, column_kind::partition_key)
                .with_column("host_id", uuid_type, column_kind::clustering_key)
                .with_column("status", utf8_type)
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
    }();
    return schema;
}

/* An internal table used by nodes to exchange CDC generation data. */
schema_ptr cdc_generations() {
    thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_TOPOLOGY_DESCRIPTION);
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_TOPOLOGY_DESCRIPTION, {id})
                /* The timestamp of this CDC generation. */
                .with_column("time", timestamp_type, column_kind::partition_key)
                /* The description of this CDC generation (see `cdc::topology_description`). */
                .with_column("description", cdc_generation_description_type)
                /* Expiration time of this CDC generation (or null if not expired). */
                .with_column("expired", timestamp_type)
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
    }();
    return schema;
}

/* A user-facing table providing identifiers of the streams used in CDC generations. */
schema_ptr cdc_desc() {
    thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC);
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC, {id})
                /* The timestamp of this CDC generation. */
                .with_column("time", timestamp_type, column_kind::partition_key)
                /* The set of stream identifiers used in this CDC generation. */
                .with_column("streams", cdc_streams_set_type)
                /* Expiration time of this CDC generation (or null if not expired). */
                .with_column("expired", timestamp_type)
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
    }();
    return schema;
}

schema_ptr service_levels() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::SERVICE_LEVELS);
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::SERVICE_LEVELS, std::make_optional(id))
                .with_column("service_level", utf8_type, column_kind::partition_key)
                .with_version(db::system_keyspace::generate_schema_version(id))
                .build();
    }();
    return schema;
}

static std::vector<schema_ptr> all_tables() {
    return {
        view_build_status(),
        cdc_generations(),
        cdc_desc(),
        service_levels(),
    };
}

bool system_distributed_keyspace::is_extra_durable(const sstring& cf_name) {
    return cf_name == CDC_TOPOLOGY_DESCRIPTION;
}

system_distributed_keyspace::system_distributed_keyspace(cql3::query_processor& qp, service::migration_manager& mm)
        : _qp(qp)
        , _mm(mm) {
}

static future<> add_timeout_columns_if_missing(database& db, ::service::migration_manager& mm) noexcept {
    static std::string_view timeout_columns[] {
        "read_timeout", "write_timeout", "range_read_timeout", "counter_write_timeout", "truncate_timeout", "cas_timeout", "other_timeout"
    };
    try {
        auto schema = db.find_schema(system_distributed_keyspace::NAME, system_distributed_keyspace::SERVICE_LEVELS);
        schema_builder b(schema);
        bool updated = false;
        for (const std::string_view& col_name : timeout_columns) {
            bytes options_name = to_bytes(col_name.data());
            if (schema->get_column_definition(options_name)) {
                continue;
            }
            updated = true;
            b.with_column(options_name, duration_type, column_kind::regular_column);
        }
        if (!updated) {
            return make_ready_future<>();
        }
        schema_ptr table = b.build();
        return mm.announce_column_family_update(table, false, {}, api::timestamp_type(0)).handle_exception([] (const std::exception_ptr&) {});
    } catch (...) {
        dlogger.warn("Failed to update options column in the role attributes table: {}", std::current_exception());
        return make_ready_future<>();
    }
}

future<> system_distributed_keyspace::start() {
    if (this_shard_id() != 0) {
        co_return;
    }

    try {
        auto ksm = keyspace_metadata::new_keyspace(
                NAME,
                "org.apache.cassandra.locator.SimpleStrategy",
                {{"replication_factor", "3"}},
                true);
        co_await _mm.announce_new_keyspace(ksm, api::min_timestamp);
    } catch (const exceptions::already_exists_exception& ignored) {
    }
    for (auto& table : all_tables()) {
        try {
            co_await _mm.announce_new_column_family(std::move(table), api::min_timestamp);
        } catch (const exceptions::already_exists_exception& ignored) {
        }
        if (need_preempt()) {
            co_await later();
        }
    }
    co_await add_timeout_columns_if_missing(_qp.db(), _mm);
}

future<> system_distributed_keyspace::stop() {
    return make_ready_future<>();
}

static service::query_state& internal_distributed_query_state() {
    using namespace std::chrono_literals;
    const auto t = 10s;
    static timeout_config tc{ t, t, t, t, t, t, t };
    static thread_local service::client_state cs(service::client_state::internal_tag{}, tc);
    static thread_local service::query_state qs(cs, empty_service_permit());
    return qs;
};

future<std::unordered_map<utils::UUID, sstring>> system_distributed_keyspace::view_status(sstring ks_name, sstring view_name) const {
    return _qp.execute_internal(
            format("SELECT host_id, status FROM {}.{} WHERE keyspace_name = ? AND view_name = ?", NAME, VIEW_BUILD_STATUS),
            db::consistency_level::ONE,
            internal_distributed_query_state(),
            { std::move(ks_name), std::move(view_name) },
            false).then([this] (::shared_ptr<cql3::untyped_result_set> cql_result) {
        return boost::copy_range<std::unordered_map<utils::UUID, sstring>>(*cql_result
                | boost::adaptors::transformed([] (const cql3::untyped_result_set::row& row) {
                    auto host_id = row.get_as<utils::UUID>("host_id");
                    auto status = row.get_as<sstring>("status");
                    return std::pair(std::move(host_id), std::move(status));
                }));
    });
}

future<> system_distributed_keyspace::start_view_build(sstring ks_name, sstring view_name) const {
    return db::system_keyspace::get_local_host_id().then([this, ks_name = std::move(ks_name), view_name = std::move(view_name)] (utils::UUID host_id) {
        return _qp.execute_internal(
                format("INSERT INTO {}.{} (keyspace_name, view_name, host_id, status) VALUES (?, ?, ?, ?)", NAME, VIEW_BUILD_STATUS),
                db::consistency_level::ONE,
                internal_distributed_query_state(),
                { std::move(ks_name), std::move(view_name), std::move(host_id), "STARTED" },
                false).discard_result();
    });
}

future<> system_distributed_keyspace::finish_view_build(sstring ks_name, sstring view_name) const {
    return db::system_keyspace::get_local_host_id().then([this, ks_name = std::move(ks_name), view_name = std::move(view_name)] (utils::UUID host_id) {
        return _qp.execute_internal(
                format("UPDATE {}.{} SET status = ? WHERE keyspace_name = ? AND view_name = ? AND host_id = ?", NAME, VIEW_BUILD_STATUS),
                db::consistency_level::ONE,
                internal_distributed_query_state(),
                { "SUCCESS", std::move(ks_name), std::move(view_name), std::move(host_id) },
                false).discard_result();
    });
}

future<> system_distributed_keyspace::remove_view(sstring ks_name, sstring view_name) const {
    return _qp.execute_internal(
            format("DELETE FROM {}.{} WHERE keyspace_name = ? AND view_name = ?", NAME, VIEW_BUILD_STATUS),
            db::consistency_level::ONE,
            internal_distributed_query_state(),
            { std::move(ks_name), std::move(view_name) },
            false).discard_result();
}

/* We want to make sure that writes/reads to/from cdc_generations and cdc_streams
 * are consistent: a read following an acknowledged write to the same partition should contact
 * at least one of the replicas that the write contacted.
 * Normally we would achieve that by always using CL = QUORUM,
 * but there's one special case when that's impossible: a single-node cluster. In that case we'll
 * use CL = ONE for writing the data, which will do the right thing -- saving the data in the only
 * possible replica. Until another node joins, reads will also use CL = ONE, retrieving the data
 * from the only existing replica.
 *
 * There is one case where queries wouldn't see the read: if we extend the single-node cluster
 * with two nodes without bootstrapping (so the data won't be streamed to new replicas),
 * and the admin forgets to run repair. Then QUORUM reads might contact only the two new nodes
 * and miss the written entry.
 *
 * Fortunately (aside from the fact that nodes shouldn't be joined without bootstrapping),
 * after the second node joins, it will propose a new CDC generation, so the old entry
 * that was written with CL=ONE won't be used by the cluster anymore. All nodes other than
 * the first one use QUORUM to make the write.
 *
 * And even if the old entry was still needed for some reason, by the time the third node joins,
 * the second node would have already fixed our issue by running read repair on the old entry.
 */
static db::consistency_level quorum_if_many(size_t num_token_owners) {
    return num_token_owners > 1 ? db::consistency_level::QUORUM : db::consistency_level::ONE;
}

static list_type_impl::native_type prepare_cdc_generation_description(const cdc::topology_description& description) {
    list_type_impl::native_type ret;
    for (auto& e: description.entries()) {
        list_type_impl::native_type streams;
        for (auto& s: e.streams) {
            streams.push_back(data_value(s.to_bytes()));
        }

        ret.push_back(make_tuple_value(cdc_token_range_description_type,
                { data_value(dht::token::to_int64(e.token_range_end))
                , make_list_value(cdc_streams_list_type, std::move(streams))
                , data_value(int8_t(e.sharding_ignore_msb))
                }));
    }
    return ret;
}

static std::vector<cdc::stream_id> get_streams_from_list_value(const data_value& v) {
    std::vector<cdc::stream_id> ret;
    auto& list_val = value_cast<list_type_impl::native_type>(v);
    for (auto& s_val: list_val) {
        ret.push_back(value_cast<bytes>(s_val));
    }
    return ret;
}

static cdc::token_range_description get_token_range_description_from_value(const data_value& v) {
    auto& tup = value_cast<tuple_type_impl::native_type>(v);
    if (tup.size() != 3) {
        on_internal_error(cdc_log, "get_token_range_description_from_value: stream tuple type size != 3");
    }

    auto token = dht::token::from_int64(value_cast<int64_t>(tup[0]));
    auto streams = get_streams_from_list_value(tup[1]);
    auto sharding_ignore_msb = uint8_t(value_cast<int8_t>(tup[2]));

    return {std::move(token), std::move(streams), sharding_ignore_msb};
}

future<>
system_distributed_keyspace::insert_cdc_topology_description(
        db_clock::time_point time,
        const cdc::topology_description& description,
        context ctx) {
    return _qp.execute_internal(
            format("INSERT INTO {}.{} (time, description) VALUES (?,?)", NAME, CDC_TOPOLOGY_DESCRIPTION),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { time, make_list_value(cdc_generation_description_type, prepare_cdc_generation_description(description)) },
            false).discard_result();
}

future<std::optional<cdc::topology_description>>
system_distributed_keyspace::read_cdc_topology_description(
        db_clock::time_point time,
        context ctx) {
    return _qp.execute_internal(
            format("SELECT description FROM {}.{} WHERE time = ?", NAME, CDC_TOPOLOGY_DESCRIPTION),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { time },
            false
    ).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) -> std::optional<cdc::topology_description> {
        if (cql_result->empty() || !cql_result->one().has("description")) {
            return {};
        }

        std::vector<cdc::token_range_description> entries;

        auto entries_val = value_cast<list_type_impl::native_type>(
                cdc_generation_description_type->deserialize(cql_result->one().get_view("description")));
        for (const auto& e_val: entries_val) {
            entries.push_back(get_token_range_description_from_value(e_val));
        }

        return { std::move(entries) };
    });
}

future<>
system_distributed_keyspace::expire_cdc_topology_description(
        db_clock::time_point streams_ts,
        db_clock::time_point expiration_time,
        context ctx) {
    return _qp.execute_internal(
            format("UPDATE {}.{} SET expired = ? WHERE time = ?", NAME, CDC_TOPOLOGY_DESCRIPTION),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { expiration_time, streams_ts },
            false).discard_result();
}

static set_type_impl::native_type prepare_cdc_streams(const std::vector<cdc::stream_id>& streams) {
    set_type_impl::native_type ret;
    for (auto& s: streams) {
        ret.push_back(data_value(s.to_bytes()));
    }
    return ret;
}

future<>
system_distributed_keyspace::create_cdc_desc(
        db_clock::time_point time,
        const std::vector<cdc::stream_id>& streams,
        context ctx) {
    return _qp.execute_internal(
            format("INSERT INTO {}.{} (time, streams) VALUES (?,?)", NAME, CDC_DESC),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { time, make_set_value(cdc_streams_set_type, prepare_cdc_streams(streams)) },
            false).discard_result();
}

future<>
system_distributed_keyspace::expire_cdc_desc(
        db_clock::time_point streams_ts,
        db_clock::time_point expiration_time,
        context ctx) {
    return _qp.execute_internal(
            format("UPDATE {}.{} SET expired = ? WHERE time = ?", NAME, CDC_DESC),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { expiration_time, streams_ts },
            false).discard_result();
}

future<bool>
system_distributed_keyspace::cdc_desc_exists(
        db_clock::time_point streams_ts,
        context ctx) {
    return _qp.execute_internal(
            format("SELECT time FROM {}.{} WHERE time = ?", NAME, CDC_DESC),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { streams_ts },
            false
    ).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) -> bool {
        return !cql_result->empty() && cql_result->one().has("time");
    });
}

future<std::map<db_clock::time_point, cdc::streams_version>> 
system_distributed_keyspace::cdc_get_versioned_streams(context ctx) {
    return _qp.execute_internal(
            format("SELECT * FROM {}.{}", NAME, CDC_DESC),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            {},
            false
    ).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) {
        std::map<db_clock::time_point, cdc::streams_version> result;

        for (auto& row : *cql_result) {
            auto ts = row.get_as<db_clock::time_point>("time");
            auto exp = row.get_opt<db_clock::time_point>("expired");
            std::vector<cdc::stream_id> ids;
            row.get_list_data<bytes>("streams", std::back_inserter(ids)); 
            result.emplace(ts, cdc::streams_version(std::move(ids), ts, exp));
        }

        return result;
    });
}

static std::optional<lowres_clock::duration> get_duration(const cql3::untyped_result_set_row&row, std::string_view col_name) {
    auto dur_opt = row.get_opt<cql_duration>(col_name);
    if (!dur_opt) {
        return std::nullopt;
    }
    return std::chrono::duration_cast<lowres_clock::duration>(std::chrono::nanoseconds(dur_opt->nanoseconds));
};

future<qos::service_levels_info> system_distributed_keyspace::get_service_levels() const {
    static sstring prepared_query = format("SELECT * FROM {}.{};", NAME, SERVICE_LEVELS);

    return _qp.execute_internal(prepared_query, {}).then([] (shared_ptr<cql3::untyped_result_set> result_set) {
        qos::service_levels_info service_levels;
        for (auto &&row : *result_set) {
            auto service_level_name = row.get_as<sstring>("service_level");
            qos::service_level_options slo{
                .read_timeout = get_duration(row, "read_timeout"),
                .write_timeout = get_duration(row, "write_timeout"),
                .range_read_timeout = get_duration(row, "range_read_timeout"),
                .counter_write_timeout = get_duration(row, "reacounter_write_timeoutd_timeout"),
                .truncate_timeout = get_duration(row, "truncate_timeout"),
                .cas_timeout = get_duration(row, "cas_timeout"),
                .other_timeout = get_duration(row, "other_timeout"),
            };
            service_levels.emplace(service_level_name, slo);
        }
        return service_levels;
    });
}

future<qos::service_levels_info> system_distributed_keyspace::get_service_level(sstring service_level_name) const {
    static sstring prepared_query = format("SELECT * FROM {}.{} WHERE service_level = ?;", NAME, SERVICE_LEVELS);
    return _qp.execute_internal(prepared_query, {service_level_name}).then([] (shared_ptr<cql3::untyped_result_set> result_set) {
        qos::service_levels_info service_levels;
        if (!result_set->empty()) {
            auto &&row = result_set->one();
            auto service_level_name = row.get_as<sstring>("service_level");
            qos::service_level_options slo{
                .read_timeout = get_duration(row, "read_timeout"),
                .write_timeout = get_duration(row, "write_timeout"),
                .range_read_timeout = get_duration(row, "range_read_timeout"),
                .counter_write_timeout = get_duration(row, "reacounter_write_timeoutd_timeout"),
                .truncate_timeout = get_duration(row, "truncate_timeout"),
                .cas_timeout = get_duration(row, "cas_timeout"),
                .other_timeout = get_duration(row, "other_timeout"),
            };
            service_levels.emplace(service_level_name, slo);
        }
        return service_levels;
    });
}

future<> system_distributed_keyspace::set_service_level(sstring service_level_name, qos::service_level_options slo) const {
    static sstring prepared_query = format("INSERT INTO {}.{} (service_level) VALUES (?);", NAME, SERVICE_LEVELS);
    co_await _qp.execute_internal(prepared_query, {service_level_name});
    auto to_data_value = [&] (const std::optional<lowres_clock::duration>& d) {
        return d
                ? data_value(cql_duration(months_counter{0}, days_counter{0}, nanoseconds_counter{std::chrono::duration_cast<std::chrono::nanoseconds>(*d).count()}))
                : data_value::make_null(duration_type);
    };
    co_await _qp.execute_internal(format("UPDATE {}.{} SET read_timeout = ? "
                ", write_timeout = ?, range_read_timeout = ?, counter_write_timeout = ? "
                ", truncate_timeout = ?, cas_timeout = ?, other_timeout = ? "
                "WHERE service_level = ?;", NAME, SERVICE_LEVELS),
                {to_data_value(slo.read_timeout),  to_data_value(slo.write_timeout),  to_data_value(slo.range_read_timeout),
                to_data_value(slo.counter_write_timeout),  to_data_value(slo.truncate_timeout),
                to_data_value(slo.cas_timeout),  to_data_value(slo.other_timeout), service_level_name});
    co_return;
}

future<> system_distributed_keyspace::drop_service_level(sstring service_level_name) const {
    static sstring prepared_query = format("DELETE FROM {}.{} WHERE service_level= ?;", NAME, SERVICE_LEVELS);
    return _qp.execute_internal(prepared_query, {service_level_name}).discard_result();
}

}
