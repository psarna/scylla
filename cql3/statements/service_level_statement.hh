/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "prepared_statement.hh"
#include "raw/parsed_statement.hh"
#include "transport/messages_fwd.hh"

namespace cql3 {

namespace statements {

///
/// A logical argument error for a service_level statement operation.
///
class service_level_argument_exception : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

///
/// An exception to indicate that the service level given as parameter doesn't exist.
///
class nonexitent_service_level_exception : public service_level_argument_exception {
public:
    nonexitent_service_level_exception(sstring service_level_name)
            : service_level_argument_exception(format("Service Level {} doesn't exists.", service_level_name)) {
    }
};




class service_level_statement : public raw::parsed_statement, public cql_statement_no_metadata {
public:
    service_level_statement() : cql_statement_no_metadata(&timeout_config::other_timeout) {}

    uint32_t get_bound_terms() const override;

    bool depends_on_keyspace(const sstring& ks_name) const override;

    bool depends_on_column_family(const sstring& cf_name) const override;

    future<> check_access(service::storage_proxy& sp, const service::client_state& state) const override;

    void validate(service::storage_proxy&, const service::client_state& state) const override;
};

}
}
