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

#include "secondary_index.hh"
#include "index/target_parser.hh"

#include <regex>
#include <boost/range/algorithm/find_if.hpp>

const sstring db::index::secondary_index::custom_index_option_name = "class_name";
const sstring db::index::secondary_index::index_keys_option_name = "index_keys";
const sstring db::index::secondary_index::index_values_option_name = "index_values";
const sstring db::index::secondary_index::index_entries_option_name = "index_keys_and_values";

namespace secondary_index {

static const std::regex target_regex("^(keys|entries|values|full)\\((.+)\\)$");
// Partition key and clustering key representation: (PK),CK
static const std::regex pk_ck_target_regex("^\\(((\\\\.|[^\\)\\\\])+)\\),((\\\\.|[^,\\)\\\\])+)$");
// Key columns representation: c1,c2,c3,c4,c5
static const std::regex key_target_regex("^((\\\\.|[^,\\)\\\\])+)(,((\\\\.|[^,\\)\\\\])+))*$");

static sstring escape(const sstring& targets) {
    static thread_local std::regex unescaped_comma(",");
    static thread_local std::regex unescaped_end_parentheses("\\)");
    static thread_local std::regex unescaped_backslash("\\\\");
    std::string result(targets);
    result = std::regex_replace(result, unescaped_backslash, "\\\\");
    result = std::regex_replace(result, unescaped_comma, "\\,");
    result = std::regex_replace(result, unescaped_end_parentheses, "\\)");
    return std::move(result);
}

static sstring unescape(const sstring& targets) {
    static thread_local std::regex escaped_comma("\\\\,");
    static thread_local std::regex escaped_end_parentheses("\\\\\\)");
    static thread_local std::regex escaped_backslash("\\\\\\\\"); // that's because \ is a special char in both C++ and regex
    std::string result(targets);
    result = std::regex_replace(result, escaped_backslash, "\\");
    result = std::regex_replace(result, escaped_comma, ",");
    result = std::regex_replace(result, escaped_end_parentheses, ")");
    return std::move(result);
}

static bool is_regular_name(const sstring& target) {
    return boost::find_if(target, [] (char c) { return c == ',' || c == ')' || c == '\\'; }) == target.end();
}

target_parser::target_info target_parser::parse(schema_ptr schema, const index_metadata& im) {
    sstring target = im.options().at(cql3::statements::index_target::target_option_name);
    try {
        return parse(schema, target);
    } catch (...) {
        throw exceptions::configuration_exception(format("Unable to parse targets for index {} ({}): {}", im.name(), target, std::current_exception()));
    }
}

target_parser::target_info target_parser::parse(schema_ptr schema, const sstring& target) {
    using namespace cql3::statements;
    target_info info;

    auto get_column = [&schema] (const sstring& name) -> const column_definition* {
        const sstring& column_name = is_regular_name(name) ? name : unescape(name);
        const column_definition* cdef = schema->get_column_definition(utf8_type->decompose(column_name));
        if (!cdef) {
            throw std::runtime_error(format("Column {} not found", column_name));
        }
        return cdef;
    };

    std::cmatch match;
    if (std::regex_match(target.data(), match, target_regex)) {
        info.type = index_target::from_sstring(match[1].str());
        info.pk_columns.push_back(get_column(sstring(match[2].str())));
    } else if (std::regex_match(target.data(), match, pk_ck_target_regex)) {
        auto pk_match = match[1].str();
        auto ck_match = match[3].str();

        auto end = std::sregex_token_iterator();
        for (auto it = std::sregex_token_iterator(pk_match.begin(), pk_match.end(), key_target_regex, {1, 4}); it != end; ++it) {
            auto column_name = it->str();
            if (column_name.empty()) {
                continue;
            }
            info.pk_columns.push_back(get_column(column_name));
        }
        for (auto it = std::sregex_token_iterator(ck_match.begin(), ck_match.end(), key_target_regex, {1, 4}); it != end; ++it) {
            auto column_name = it->str();
            if (column_name.empty()) {
                continue;
            }
            info.ck_columns.push_back(get_column(column_name));
        }
        info.type = index_target::target_type::values;
    } else {
        info.pk_columns.push_back(get_column(target));
        info.type = index_target::target_type::values;
    }

    return info;
}

}
