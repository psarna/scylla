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
 * Copyright (C) 2019 ScyllaDB
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

#include <vector>

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include "cql3/query_options.hh"
#include "cql3/term.hh"
#include "cql3/statements/bound.hh"
#include "index/secondary_index_manager.hh"
#include "types.hh"

namespace cql3 {

namespace restrictions {

struct allow_local_index_tag {};
using allow_local_index = bool_class<allow_local_index_tag>;

/**
 * Base class for <code>Restriction</code>s
 */
class restriction {
public:
    virtual ~restriction() {}

    virtual bool is_on_token() const {
        return false;
    }

    virtual bool is_multi_column() const {
        return false;
    }

    virtual bool is_slice() const {
        return false;
    }

    virtual bool is_EQ() const {
        return false;
    }

    virtual bool is_IN() const {
        return false;
    }

    virtual bool is_contains() const {
        return false;
    }

    virtual bool is_LIKE() const {
        return false;
    }

    /**
     * Checks if the specified bound is set or not.
     * @param b the bound type
     * @return <code>true</code> if the specified bound is set, <code>false</code> otherwise
     */
    virtual bool has_bound(statements::bound b) const {
        return true;
    }

    virtual std::vector<bytes_opt> bounds(statements::bound b, const query_options& options) const {
        return values(options);
    }

    virtual bool is_inclusive(statements::bound b) const {
        return true;
    }

    /**
     * Merges this restriction with the specified one.
     *
     * @param otherRestriction the restriction to merge into this one
     * @return the restriction resulting of the merge
     * @throws InvalidRequestException if the restrictions cannot be merged
     */
    virtual void merge_with(::shared_ptr<restriction> other) = 0;

    /**
     * Check if the restriction is on indexed columns.
     *
     * @param indexManager the index manager
     * @return <code>true</code> if the restriction is on indexed columns, <code>false</code>
     */
    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager, allow_local_index allow_local) const = 0;

    virtual sstring to_string() const = 0;

    /**
     * Returns <code>true</code> if one of the restrictions use the specified function.
     *
     * @param ks_name the keyspace name
     * @param function_name the function name
     * @return <code>true</code> if one of the restrictions use the specified function, <code>false</code> otherwise.
     */
    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const = 0;

    virtual std::vector<bytes_opt> values(const query_options& options) const = 0;

    virtual bytes_opt value(const query_options& options) const {
        auto vec = values(options);
        assert(vec.size() == 1);
        return std::move(vec[0]);
    }

    /**
     * Whether the specified row satisfied this restriction.
     * Assumes the row is live, but not all cells. If a cell
     * isn't live and there's a restriction on its column,
     * then the function returns false.
     *
     * @param schema the schema the row belongs to
     * @param key the partition key
     * @param ckey the clustering key
     * @param cells the remaining row columns
     * @return the restriction resulting of the merge
     * @throws InvalidRequestException if the restrictions cannot be merged
     */
    virtual bool is_satisfied_by(const schema& schema,
                                 const partition_key& key,
                                 const clustering_key_prefix& ckey,
                                 const row& cells,
                                 const query_options& options,
                                 gc_clock::time_point now) const = 0;

protected:
    /**
     * Checks if the specified term is using the specified function.
     *
     * @param term the term to check
     * @param ks_name the function keyspace name
     * @param function_name the function name
     * @return <code>true</code> if the specified term is using the specified function, <code>false</code> otherwise.
     */
    static bool term_uses_function(::shared_ptr<term> term, const sstring& ks_name, const sstring& function_name) {
        return bool(term) && term->uses_function(ks_name, function_name);
    }

    /**
     * Checks if one of the specified term is using the specified function.
     *
     * @param terms the terms to check
     * @param ks_name the function keyspace name
     * @param function_name the function name
     * @return <code>true</code> if one of the specified term is using the specified function, <code>false</code> otherwise.
     */
    static bool term_uses_function(const std::vector<::shared_ptr<term>>& terms, const sstring& ks_name, const sstring& function_name) {
        for (auto&& value : terms) {
            if (term_uses_function(value, ks_name, function_name)) {
                return true;
            }
        }
        return false;
    }
};

}

}
