/*
 * Copyright 2019 ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * This file contains definitions and functions related to placing conditions
 * on Alternator queries (equivalent of CQL's restrictions).
 *
 * With conditions, it's possible to add criteria to selection requests (Scan, Query)
 * and use them for narrowing down the result set, by means of filtering or indexing.
 *
 * Ref: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html
 */

#pragma once

#include "cql3/restrictions/statement_restrictions.hh"
#include "serialization.hh"
#include "alternator/error.hh"

namespace alternator {

enum class comparison_operator_type {
    EQ, NE, LE, LT, GE, GT, IN, BETWEEN, CONTAINS, NOT_CONTAINS, IS_NULL, NOT_NULL, BEGINS_WITH
};

comparison_operator_type get_comparison_operator(const rjson::value& comparison_operator);

::shared_ptr<cql3::restrictions::statement_restrictions> get_filtering_restrictions(schema_ptr schema, const column_definition& attrs_col, const rjson::value& query_filter);

std::optional<api_error> verify_expected(const rjson::value& req, const std::unique_ptr<rjson::value>& previous_item);

}
