/*
 * Copyright (C) 2021 ScyllaDB
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

#include "seastarx.hh"
#include "cql3/statements/list_service_level_statement.hh"
#include "service/qos/service_level_controller.hh"
#include "transport/messages/result_message.hh"
#include "utils/overloaded_functor.hh"

namespace cql3 {

namespace statements {

list_service_level_statement::list_service_level_statement(sstring service_level, bool describe_all) :
    _service_level(service_level), _describe_all(describe_all) {
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::list_service_level_statement::prepare(
        database &db, cql_stats &stats) {
    return std::make_unique<prepared_statement>(::make_shared<list_service_level_statement>(*this));
}

void list_service_level_statement::validate(service::storage_proxy &, const service::client_state &) const {
}

future<> list_service_level_statement::check_access(service::storage_proxy& sp, const service::client_state &state) const {
    return state.ensure_has_permission(auth::command_desc{.permission = auth::permission::DESCRIBE, .resource = auth::root_service_level_resource()});
}

future<::shared_ptr<cql_transport::messages::result_message>>
list_service_level_statement::execute(query_processor& qp,
        service::query_state &state,
        const query_options &) const {

    static auto make_column = [] (sstring name, const shared_ptr<const abstract_type> type) {
        return make_lw_shared<column_specification>(
                "QOS",
                "service_levels",
                ::make_shared<column_identifier>(std::move(name), true),
                type);
    };

    static thread_local const std::vector<lw_shared_ptr<column_specification>> metadata({make_column("service_level", utf8_type),
        make_column("timeout", duration_type),
        make_column("workload_type", utf8_type)
    });

    return make_ready_future().then([this, &state] () {
                                  if (_describe_all) {
                                      return state.get_service_level_controller().get_distributed_service_levels();
                                  } else {
                                      return state.get_service_level_controller().get_distributed_service_level(_service_level);
                                  }
                              })
            .then([this] (qos::service_levels_info sl_info) {
                auto d = [] (const qos::service_level_options::timeout_type& duration) -> bytes_opt {
                    return std::visit(overloaded_functor{
                        [&] (const qos::service_level_options::unset_marker&) {
                            return bytes_opt();
                        },
                        [&] (const qos::service_level_options::delete_marker&) {
                            return bytes_opt();
                        },
                        [&] (const lowres_clock::duration& d) -> bytes_opt {
                            auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
                            return duration_type->decompose(cql_duration(months_counter{0}, days_counter{0}, nanoseconds_counter{nanos}));
                        },
                    }, duration);
                };
                auto rs = std::make_unique<result_set>(metadata);
                for (auto &&[sl_name, slo] : sl_info) {
                    rs->add_row(std::vector<bytes_opt>{
                            utf8_type->decompose(sl_name),
                            d(slo.timeout),
                            utf8_type->decompose(qos::service_level_options::to_string(slo.workload))});
                }

                auto rows = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(std::move(rs))));
                return ::static_pointer_cast<cql_transport::messages::result_message>(rows);
            });
}
}
}
