/*
 * Copyright (C) 2014 ScyllaDB
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

#include "types.hh"
#include "types/tuple.hh"

class user_type_impl : public tuple_type_impl {
    using intern = type_interning_helper<user_type_impl, sstring, bytes, std::vector<bytes>, std::vector<data_type>>;
public:
    const sstring _keyspace;
    const bytes _name;
private:
    std::vector<bytes> _field_names;
    std::vector<sstring> _string_field_names;
public:
    using native_type = std::vector<data_value>;
    user_type_impl(sstring keyspace, bytes name, std::vector<bytes> field_names, std::vector<data_type> field_types)
            : tuple_type_impl(make_name(keyspace, name, field_names, field_types, false /* frozen */), field_types)
            , _keyspace(keyspace)
            , _name(name)
            , _field_names(field_names) {
        for (const auto& field_name : _field_names) {
            _string_field_names.emplace_back(utf8_type->to_string(field_name));
        }
    }
    static shared_ptr<const user_type_impl> get_instance(sstring keyspace, bytes name, std::vector<bytes> field_names, std::vector<data_type> field_types) {
        return intern::get_instance(std::move(keyspace), std::move(name), std::move(field_names), std::move(field_types));
    }
    data_type field_type(size_t i) const { return type(i); }
    const std::vector<data_type>& field_types() const { return _types; }
    bytes_view field_name(size_t i) const { return _field_names[i]; }
    sstring field_name_as_string(size_t i) const { return _string_field_names[i]; }
    const std::vector<bytes>& field_names() const { return _field_names; }
    sstring get_name_as_string() const;
    virtual sstring cql3_type_name_impl() const override;
    virtual bool is_native() const override { return false; }
    virtual bool equals(const abstract_type& other) const override;
    virtual bool is_user_type() const override { return true; }
    virtual bool references_user_type(const sstring& keyspace, const bytes& name) const override;
    virtual std::optional<data_type> update_user_type(const shared_ptr<const user_type_impl> updated) const override;

    virtual sstring to_json_string(bytes_view bv) const override;
    virtual bytes from_json_object(const Json::Value& value, cql_serialization_format sf) const override;
private:
    static sstring make_name(sstring keyspace,
                             bytes name,
                             std::vector<bytes> field_names,
                             std::vector<data_type> field_types,
                             bool is_multi_cell);
};

data_value make_user_value(data_type tuple_type, user_type_impl::native_type value);

