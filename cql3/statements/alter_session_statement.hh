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

#pragma once

#include "transport/messages_fwd.hh"
#include "cql3/cql_statement.hh"
#include "cql3/statements/raw/parsed_statement.hh"
#include "cql3/statements/alter_session_prop_defs.hh"
#include "prepared_statement.hh"

namespace cql3 {

namespace statements {

class alter_session_statement : public cql_statement_no_metadata, public raw::parsed_statement {
private:
    const ::shared_ptr<alter_session_prop_defs> _props;
    std::optional<sstring> _to_delete;

public:
    explicit alter_session_statement(::shared_ptr<alter_session_prop_defs> props);
    explicit alter_session_statement(sstring to_delete);

    virtual uint32_t get_bound_terms() const override;
    virtual bool depends_on_keyspace(const sstring& ks_name) const override;
    virtual bool depends_on_column_family(const sstring& cf_name) const override;
    virtual future<> check_access(service::storage_proxy& proxy, const service::client_state& state) const override;
    virtual void validate(service::storage_proxy&, const service::client_state& state) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(service::storage_proxy& proxy, service::query_state& state, const query_options& options) const override;

    // This is a self-preparing statement, since it doesn't really need a separate raw form
    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;
};

}

}
