/*
 * Copyright 2020 ScyllaDB
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

#pragma once

#include "string_view"
#include "rjson.hh"
#include "seastar/core/future.hh"
#include "seastar/core/abort_source.hh"

namespace alternator {

class json_parser {
    std::string_view _raw_document;
    rjson::value _parsed_document;
    std::exception_ptr _current_exception;
    semaphore _parsing_sem{1};
    condition_variable _document_waiting;
    condition_variable _document_parsed;
    abort_source _as;
    future<> _started;
public:
    json_parser();
    future<rjson::value> parse(std::string_view content);
    future<> stop();
};

}
