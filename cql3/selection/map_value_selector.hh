/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "selector.hh"
#include "types.hh"
#include "types/map.hh"

namespace cql3 {

namespace selection {

class map_value_selector : public selector {
    map_type _type;
    bytes _key;
    shared_ptr<selector> _selected;
public:
    static shared_ptr<factory> new_factory(map_type type, bytes key, shared_ptr<selector::factory> factory) {
        struct field_selector_factory : selector::factory {
            map_type _type;
            bytes _key;
            shared_ptr<selector::factory> _factory;

            field_selector_factory(map_type type, bytes key, shared_ptr<selector::factory> factory)
                    : _type(std::move(type)), _key(std::move(key)), _factory(std::move(factory)) {
            }

            virtual sstring column_name() override {
                auto kname = sstring(reinterpret_cast<const char*>(_key.begin()), _key.size());
                return format("{}[{}]", _factory->column_name(), kname);
            }

            virtual data_type get_return_type() override {
                return _type->get_values_type();
            }

            shared_ptr<selector> new_instance() override {
                return make_shared<map_value_selector>(_type, _key, _factory->new_instance());
            }

            bool is_aggregate_selector_factory() override {
                return _factory->is_aggregate_selector_factory();
            }
        };
        return make_shared<field_selector_factory>(std::move(type), std::move(key), std::move(factory));
    }

    virtual bool is_aggregate() override {
        return false;
    }

    virtual void add_input(cql_serialization_format sf, result_set_builder& rs) override {
        _selected->add_input(sf, rs);
    }

    virtual bytes_opt get_output(cql_serialization_format sf) override {
        // FIXME(sarna): get element from col
        return bytes_opt{};
        /*auto&& value = _selected->get_output(sf);
        if (!value) {
            return std::nullopt;
        }
        auto&& buffers = _type->split(*value);
        bytes_opt ret;
        if (_field < buffers.size() && buffers[_field]) {
            ret = to_bytes(*buffers[_field]);
        }
        return ret;*/
    }

    virtual data_type get_type() override {
        return _type->get_values_type();
    }

    virtual void reset() {
        _selected->reset();
    }

    virtual sstring assignment_testable_source_context() const override {
        auto kname = sstring(reinterpret_cast<const char*>(_key.begin()), _key.size());
        return format("{}[{}]", _selected, kname);
    }

    map_value_selector(map_type type, bytes key, shared_ptr<selector> selected)
            : _type(std::move(type)), _key(std::move(key)), _selected(std::move(selected)) {
    }
};

}
}
