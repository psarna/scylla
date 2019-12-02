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

#include <stdexcept>
#include "index_target.hh"
#include "index/secondary_index.hh"
#include <boost/algorithm/string/join.hpp>
#include "types/map.hh"

namespace cql3 {

::shared_ptr<index_target_identifier> index_target_identifier::raw::prepare(schema_ptr s) const {
    auto ident = raw_ident->prepare_column_identifier(s);
    if (raw_key) {
        const column_definition* map_column = s->get_column_definition(ident->name());
        if (!map_column || !map_column->is_multi_cell()) {
            throw std::runtime_error("Indexing an element of a non-collection column is not supported");
        }
        auto collection_type = dynamic_pointer_cast<const map_type_impl>(map_column->type);
        if (!collection_type) {
            throw std::runtime_error("Indexing an element is currently only supported for maps");
        }
        data_type key_type = collection_type->get_keys_type();
        auto key_constant = static_pointer_cast<constants::value>(raw_key->prepare_as(key_type));
        return ::make_shared<index_target_identifier>(std::move(ident), std::move(key_constant));
    } else {
        return ::make_shared<index_target_identifier>(std::move(ident));
    }
}

sstring index_target_identifier::to_string() const {
    return ident->to_string() + (bool(collection_key) ? "_entry" : "");
}

Json::Value index_target_identifier::to_json() const {
    if (collection_key) {
        map_value_column_computation computation(ident->name(), *collection_key->_bytes.data());
        return computation.to_json();
    }
    return Json::Value(ident->to_string());
}

namespace statements {

using db::index::secondary_index;

const sstring index_target::target_option_name = "target";
const sstring index_target::custom_index_option_name = "class_name";

sstring index_target::as_string() const {
    struct as_string_visitor {
        sstring operator()(const multiple_columns& columns) const {
            return "(" + boost::algorithm::join(columns | boost::adaptors::transformed(
                    [](const single_column& ident) -> sstring {
                        return ident->to_string();
                    }), ",") + ")";
        }

        sstring operator()(const single_column& column) const {
            return column->to_string();
        }
    };

    return std::visit(as_string_visitor(), value);
}

index_target::target_type index_target::from_sstring(const sstring& s)
{
    if (s == "keys") {
        return index_target::target_type::keys;
    } else if (s == "entries") {
        return index_target::target_type::keys_and_values;
    } else if (s == "values") {
        return index_target::target_type::values;
    } else if (s == "full") {
        return index_target::target_type::full;
    }
    throw std::runtime_error(format("Unknown target type: {}", s));
}

sstring index_target::index_option(target_type type) {
    switch (type) {
        case target_type::keys: return secondary_index::index_keys_option_name;
        case target_type::keys_and_values: return secondary_index::index_entries_option_name;
        case target_type::values: return secondary_index::index_values_option_name;
        default: throw std::invalid_argument("should not reach");
    }
}

::shared_ptr<index_target::raw>
index_target::raw::values_of(::shared_ptr<index_target_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::values);
}

::shared_ptr<index_target::raw>
index_target::raw::keys_of(::shared_ptr<index_target_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::keys);
}

::shared_ptr<index_target::raw>
index_target::raw::keys_and_values_of(::shared_ptr<index_target_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::keys_and_values);
}

::shared_ptr<index_target::raw>
index_target::raw::full_collection(::shared_ptr<index_target_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::full);
}

::shared_ptr<index_target::raw>
index_target::raw::columns(std::vector<::shared_ptr<index_target_identifier::raw>> c) {
    return ::make_shared<raw>(std::move(c), target_type::values);
}

::shared_ptr<index_target>
index_target::raw::prepare(schema_ptr schema) {
    struct prepare_visitor {
        schema_ptr _schema;
        target_type _type;

        ::shared_ptr<index_target> operator()(const multiple_columns& columns) const {
            auto prepared_idents = boost::copy_range<std::vector<::shared_ptr<index_target_identifier>>>(
                    columns | boost::adaptors::transformed([this] (const raw::single_column& raw_ident) {
                        return raw_ident->prepare(_schema);
                    })
            );
            return ::make_shared<index_target>(std::move(prepared_idents), _type);
        }

        ::shared_ptr<index_target> operator()(const single_column& raw_ident) const {
            return ::make_shared<index_target>(raw_ident->prepare(_schema), _type);
        }
    };

    return std::visit(prepare_visitor{schema, type}, value);
}

sstring to_sstring(index_target::target_type type)
{
    switch (type) {
    case index_target::target_type::keys: return "keys";
    case index_target::target_type::keys_and_values: return "entries";
    case index_target::target_type::values: return "values";
    case index_target::target_type::full: return "full";
    }
    return "";
}

}

}
