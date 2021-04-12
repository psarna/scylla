/*
 * Copyright (C) 2020 ScyllaDB
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

#include <algorithm>
#include "service_level_controller.hh"
#include "service/storage_service.hh"
#include "service/priority_manager.hh"
#include "message/messaging_service.hh"
#include "db/system_distributed_keyspace.hh"

namespace qos {

sstring service_level_controller::default_service_level_name = "default";



service_level_controller::service_level_controller(sharded<auth::service>& auth_service, service_level_options default_service_level_config):
        _sl_data_accessor(nullptr),
        _auth_service(auth_service)
{
    if (this_shard_id() == global_controller) {
        _global_controller_db = std::make_unique<global_controller_data>();
        _global_controller_db->default_service_level_config = default_service_level_config;
    }
}

future<> service_level_controller::add_service_level(sstring name, service_level_options slo, bool is_static) {
    return container().invoke_on(global_controller, [=] (service_level_controller &sl_controller) {
        return with_semaphore(sl_controller._global_controller_db->notifications_serializer, 1, [&sl_controller, name, slo, is_static] () {
           return sl_controller.do_add_service_level(name, slo, is_static);
        });
    });
}

future<>  service_level_controller::remove_service_level(sstring name, bool remove_static) {
    return container().invoke_on(global_controller, [=] (service_level_controller &sl_controller) {
        return with_semaphore(sl_controller._global_controller_db->notifications_serializer, 1, [&sl_controller, name, remove_static] () {
           return sl_controller.do_remove_service_level(name, remove_static);
        });
    });
}

future<> service_level_controller::start() {
    if (this_shard_id() != global_controller) {
        return make_ready_future();
    }
    return with_semaphore(_global_controller_db->notifications_serializer, 1, [this] () {
        return do_add_service_level(default_service_level_name, _global_controller_db->default_service_level_config, true).then([this] () {
            return container().invoke_on_all([] (service_level_controller& sl) {
                sl._default_service_level =  sl.get_service_level(default_service_level_name);
            });
        });
    });
}


void service_level_controller::set_distributed_data_accessor(service_level_distributed_data_accessor_ptr sl_data_accessor) {
    // unregistering the accessor is always legal
    if (!sl_data_accessor) {
        _sl_data_accessor = nullptr;
    }

    // Registration of a new accessor can be done only when the _sl_data_accessor is not already set.
    // This behavior is intended to allow to unit testing debug to set this value without having
    // overriden by storage_proxy
    if (!_sl_data_accessor) {
        _sl_data_accessor = sl_data_accessor;
    }
}

future<> service_level_controller::stop() {
    // unregister from the service level distributed data accessor.
    _sl_data_accessor = nullptr;
    if (this_shard_id() == global_controller) {
        // abort the loop of the distributed data checking if it is running
        _global_controller_db->dist_data_update_aborter.request_abort();
        _global_controller_db->notifications_serializer.broken();
    }
    return std::exchange(_distributed_data_updater, make_ready_future<>());
}

future<> service_level_controller::update_service_levels_from_distributed_data() {

    if (!_sl_data_accessor) {
        return make_ready_future();
    }

    return container().invoke_on(global_controller, [] (service_level_controller& sl_controller) {
        return with_semaphore(sl_controller._global_controller_db->notifications_serializer, 1, [&sl_controller] () {
            return async([&sl_controller] () {
                service_levels_info service_levels = sl_controller._sl_data_accessor->get_service_levels().get0();
                service_levels_info service_levels_for_add_or_update;
                service_levels_info service_levels_for_delete;

                auto current_it = sl_controller._service_levels_db.begin();
                auto new_state_it = service_levels.begin();

                // we want to detect 3 kinds of objects in one pass -
                // 1. new service levels that have been added to the distributed keyspace
                // 2. existing service levels that have changed
                // 3. removed service levels
                // this loop is batching together add/update operation and remove operation
                // then they are all executed together.The reason for this is to allow for
                // firstly delete all that there is to be deleted and only then adding new
                // service levels.
                while (current_it != sl_controller._service_levels_db.end() && new_state_it != service_levels.end()) {
                    if (current_it->first == new_state_it->first) {
                        //the service level exists on both the cureent and new state.
                       if (current_it->second.slo != new_state_it->second) {
                           // The service level configuration is different
                           // in the new state and the old state, meaning it needs to be updated.
                           service_levels_for_add_or_update.insert(*new_state_it);
                       }
                       current_it++;
                       new_state_it++;
                   } else if (current_it->first < new_state_it->first) {
                       //The service level does not exists in the new state so it needs to be
                       //removed, but only if it is not static since static configurations dont
                       //come from the distributed keyspace but from code.
                       if (!current_it->second.is_static) {
                           service_levels_for_delete.emplace(current_it->first, current_it->second.slo);
                       }
                       current_it++;
                   } else { /*new_it->first < current_it->first */
                       // The service level exits in the new state but not in the old state
                       // so it needs to be added.
                       service_levels_for_add_or_update.insert(*new_state_it);
                       new_state_it++;
                   }
                }

                for (; current_it != sl_controller._service_levels_db.end(); current_it++) {
                    service_levels_for_delete.emplace(current_it->first, current_it->second.slo);
                }
                std::copy(new_state_it, service_levels.end(), std::inserter(service_levels_for_add_or_update,
                        service_levels_for_add_or_update.end()));

                for (auto&& sl : service_levels_for_delete) {
                    sl_controller.do_remove_service_level(sl.first, false).get();
                }
                for (auto&& sl : service_levels_for_add_or_update) {
                    sl_controller.do_add_service_level(sl.first, sl.second).get();
                }
            });
        });
    });
}

future<std::optional<service_level_options>> service_level_controller::find_service_level(auth::role_set roles) {
    static auto sl_compare = std::less<sstring>();
    auto& role_manager = _auth_service.local().underlying_role_manager();

    // converts a list of roles into the chosen service level.
    return ::map_reduce(roles.begin(), roles.end(), [&role_manager, this] (const sstring& role) {
        return role_manager.get_attribute(role, "service_level").then_wrapped([this, role] (future<std::optional<sstring>> sl_name_fut) -> std::optional<service_level_options> {
            try {
                std::optional<sstring> sl_name = sl_name_fut.get0();
                if (!sl_name) {
                    return std::nullopt;
                }
                auto sl_it = _service_levels_db.find(*sl_name);
                if ( sl_it == _service_levels_db.end()) {
                    return std::nullopt;
                }
                return sl_it->second.slo;
            } catch (...) { // when we fail, we act as if the attribute does not exist so the node
                           // will not be brought down.
                return std::nullopt;
            }
        });
    }, std::optional<service_level_options>{}, [this] (std::optional<service_level_options> first, std::optional<service_level_options> second) -> std::optional<service_level_options> {
        if (!second) {
            return first;
        } else if (!first) {
            return second;
        } else {
            return first->merge_with(*second);
        }
    });
}

future<>  service_level_controller::notify_service_level_added(sstring name, service_level sl_data) {
    _service_levels_db.emplace(name, sl_data);
    return make_ready_future();
}

future<> service_level_controller::notify_service_level_updated(sstring name, service_level_options slo) {
    auto sl_it = _service_levels_db.find(name);
    future<> f = make_ready_future();
    if (sl_it != _service_levels_db.end()) {
        sl_it->second.slo = slo;
    }
    return f;
}

future<> service_level_controller::notify_service_level_removed(sstring name) {
    auto sl_it = _service_levels_db.find(name);
    if (sl_it != _service_levels_db.end()) {
        _service_levels_db.erase(sl_it);
    }
    return make_ready_future();
}

void service_level_controller::update_from_distributed_data(std::chrono::duration<float> interval) {
    _distributed_data_updater = container().invoke_on(global_controller, [interval] (service_level_controller& global_sl) {
        if (global_sl._global_controller_db->distributed_data_update.available()) {
            global_sl._global_controller_db->distributed_data_update = repeat([interval, &global_sl] {
                return sleep_abortable<steady_clock_type>(std::chrono::duration_cast<steady_clock_type::duration>(interval),
                        global_sl._global_controller_db->dist_data_update_aborter).then_wrapped([&global_sl] (future<>&& f) {
                        try {
                            f.get();
                            return global_sl.update_service_levels_from_distributed_data().then([] {
                                    return stop_iteration::no;
                            });
                        }
                        catch (const sleep_aborted& e) {
                            return make_ready_future<seastar::bool_class<seastar::stop_iteration_tag>>(stop_iteration::yes);
                        }
                    });
            });
        }
    });
}

future<> service_level_controller::add_distributed_service_level(sstring name, service_level_options slo, bool if_not_exists) {
    set_service_level_op_type add_type = if_not_exists ? set_service_level_op_type::add_if_not_exists : set_service_level_op_type::add;
    return set_distributed_service_level(name, slo, add_type);
}

future<> service_level_controller::alter_distributed_service_level(sstring name, service_level_options slo) {
    return set_distributed_service_level(name, slo, set_service_level_op_type::alter);
}

future<> service_level_controller::drop_distributed_service_level(sstring name, bool if_exists) {
    return _sl_data_accessor->get_service_levels().then([this, name, if_exists] (qos::service_levels_info sl_info) {
        auto it = sl_info.find(name);
        if (it == sl_info.end()) {
            if (if_exists) {
                return make_ready_future();
            } else {
                return make_exception_future(nonexistant_service_level_exception(name));
            }
        } else {
            auto& role_manager = _auth_service.local().underlying_role_manager();
            return role_manager.query_attribute_for_all("service_level").then( [&role_manager, name] (auth::role_manager::attribute_vals attributes) {
                return parallel_for_each(attributes.begin(), attributes.end(), [&role_manager, name] (auto&& attr) {
                    if (attr.second == name) {
                        return role_manager.remove_attribute(attr.first, "service_level");
                    } else {
                        return make_ready_future();
                    }
                });
            }).then([this, name] {
                return _sl_data_accessor->drop_service_level(name);
            });
        }
    });
}

future<service_levels_info> service_level_controller::get_distributed_service_levels() {
    return _sl_data_accessor->get_service_levels();
}

future<service_levels_info> service_level_controller::get_distributed_service_level(sstring service_level_name) {
    return _sl_data_accessor->get_service_level(service_level_name);
}

future<> service_level_controller::set_distributed_service_level(sstring name, service_level_options slo, set_service_level_op_type op_type) {
    return _sl_data_accessor->get_service_levels().then([this, name, slo, op_type] (qos::service_levels_info sl_info) {
        auto it = sl_info.find(name);
        // test for illegal requests or requests that should terminate without any action
        if (it == sl_info.end()) {
            if (op_type == set_service_level_op_type::alter) {
                return make_exception_future(exceptions::invalid_request_exception(format("The service level '{}' desn't exist.", name)));
            }
        } else {
            if (op_type == set_service_level_op_type::add) {
                return make_exception_future(exceptions::invalid_request_exception(format("The service level '{}' already exists.", name)));
            } else if (op_type == set_service_level_op_type::add_if_not_exists) {
                return make_ready_future();
            }
        }
        return _sl_data_accessor->set_service_level(name, slo);
    });
}

future<> service_level_controller::do_add_service_level(sstring name, service_level_options slo, bool is_static) {
    auto service_level_it = _service_levels_db.find(name);
    if (is_static) {
        _global_controller_db->static_configurations[name] = slo;
    }
    if (service_level_it != _service_levels_db.end()) {
        if ((is_static && service_level_it->second.is_static) || !is_static) {
           if ((service_level_it->second.is_static) && (!is_static)) {
               service_level_it->second.is_static = false;
           }
           return container().invoke_on_all(&service_level_controller::notify_service_level_updated, name, slo);
        } else {
            // this means we set static layer when the the service level
            // is running of the non static configuration. so we have nothing
            // else to do since we already saved the static configuration.
            return make_ready_future();
        }
    } else {
        return do_with(service_level{.slo = slo, .is_static = is_static}, std::move(name), [this] (service_level& sl, sstring& name) {
            return container().invoke_on_all(&service_level_controller::notify_service_level_added, name, sl);
        });
    }
    return make_ready_future();
}

future<> service_level_controller::do_remove_service_level(sstring name, bool remove_static) {
    auto service_level_it = _service_levels_db.find(name);
    if (service_level_it != _service_levels_db.end()) {
        auto static_conf_it = _global_controller_db->static_configurations.end();
        bool static_exists = false;
        if (remove_static) {
            _global_controller_db->static_configurations.erase(name);
        } else {
            static_conf_it = _global_controller_db->static_configurations.find(name);
            static_exists = static_conf_it != _global_controller_db->static_configurations.end();
        }
        if (remove_static && service_level_it->second.is_static) {
            return container().invoke_on_all(&service_level_controller::notify_service_level_removed, name);
        } else if (!remove_static && !service_level_it->second.is_static) {
            if (static_exists) {
                service_level_it->second.is_static = true;
                return container().invoke_on_all(&service_level_controller::notify_service_level_updated, name, static_conf_it->second);
            } else {
                return container().invoke_on_all(&service_level_controller::notify_service_level_removed, name);
            }
        }
    }
    return make_ready_future();
}

}
