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

#include <seastar/core/shared_ptr.hh>
#include "cql3/column_identifier.hh"
#include "cql3/constants.hh"
#include <variant>

namespace cql3 {

struct index_target_identifier {
    ::shared_ptr<column_identifier> ident;

    struct raw {
        ::shared_ptr<column_identifier::raw> raw_ident;
        raw(::shared_ptr<column_identifier::raw> raw_ident) : raw_ident(raw_ident) {}
        virtual ::shared_ptr<index_target_identifier> prepare(schema_ptr s) const;
        virtual bool is_computed() const { return false; };
    };

    virtual ~index_target_identifier() {}
    explicit index_target_identifier(::shared_ptr<column_identifier> ident) : ident(ident) {}

    virtual sstring to_string() const;
    virtual Json::Value to_json() const;
    virtual bool is_computed() const { return false; };
};

struct map_entry_index_target_identifier : public index_target_identifier {
    ::shared_ptr<constants::value> key;

    struct raw : index_target_identifier::raw {
        ::shared_ptr<constants::literal> raw_key;
        raw(::shared_ptr<column_identifier::raw> raw_ident, ::shared_ptr<constants::literal> raw_key) : index_target_identifier::raw(raw_ident), raw_key(raw_key) {}
        virtual ::shared_ptr<index_target_identifier> prepare(schema_ptr s) const override;
        virtual bool is_computed() const override { return true; };
    };

    map_entry_index_target_identifier(::shared_ptr<column_identifier> ident, ::shared_ptr<constants::value> k) : index_target_identifier(ident),  key(k) {}

    virtual sstring to_string() const override;
    virtual Json::Value to_json() const override;
    virtual bool is_computed() const override { return true; };
};

namespace statements {

struct index_target {
    static const sstring target_option_name;
    static const sstring custom_index_option_name;

    using single_column =::shared_ptr<index_target_identifier>;
    using multiple_columns = std::vector<::shared_ptr<index_target_identifier>>;
    using value_type = std::variant<single_column, multiple_columns>;

    enum class target_type {
        values, keys, keys_and_values, full
    };

    const value_type value;
    const target_type type;

    index_target(single_column c, target_type t) : value(c) , type(t) {}
    index_target(multiple_columns c, target_type t) : value(std::move(c)), type(t) {}

    sstring as_string() const;

    static sstring index_option(target_type type);
    static target_type from_column_definition(const column_definition& cd);
    static index_target::target_type from_sstring(const sstring& s);

    class raw {
    public:
        using single_column = ::shared_ptr<index_target_identifier::raw>;
        using multiple_columns = std::vector<::shared_ptr<index_target_identifier::raw>>;
        using value_type = std::variant<single_column, multiple_columns>;

        const value_type value;
        const target_type type;

        raw(single_column c, target_type t) : value(c), type(t) {}
        raw(multiple_columns pk_columns, target_type t) : value(pk_columns), type(t) {}

        static ::shared_ptr<raw> values_of(::shared_ptr<index_target_identifier::raw> c);
        static ::shared_ptr<raw> keys_of(::shared_ptr<index_target_identifier::raw> c);
        static ::shared_ptr<raw> keys_and_values_of(::shared_ptr<index_target_identifier::raw> c);
        static ::shared_ptr<raw> full_collection(::shared_ptr<index_target_identifier::raw> c);
        static ::shared_ptr<raw> columns(std::vector<::shared_ptr<index_target_identifier::raw>> c);
        ::shared_ptr<index_target> prepare(schema_ptr);
    };
};

sstring to_sstring(index_target::target_type type);

}
}
