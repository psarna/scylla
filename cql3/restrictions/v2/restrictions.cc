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

#include "cql3/restrictions/v2/restrictions.hh"
#include "exceptions/exceptions.hh"
#include <boost/range/adaptors.hpp>
#include "to_string.hh"

namespace cql3::restrictions::v2 {

static logging::logger rlogger("restrictions.v2");

sstring restriction::to_string() const {
    struct to_string_visitor {
        sstring operator()(const single_value& v) const {
            return v->to_string();
        }
        sstring operator()(const multiple_values& vs) const {
            return ::join(",", vs | boost::adaptors::transformed([] (::shared_ptr<term> single) { return single->to_string(); }));
        }
        sstring operator()(const map_entry& v) const {
            return format("{{{},{}}}", v.key->to_string(), v.value->to_string());
        }
        sstring operator()(const term_slice& v) const {
            return v.to_string();
        }
        sstring operator()(const ::shared_ptr<abstract_marker>& v) const {
            return v->to_string();
        }
    };

    return format("{} {} {}{}", _op, ::join(",", _target | boost::adaptors::transformed([] (const column_definition* cdef) { return cdef->name_as_text(); })), std::visit(to_string_visitor(), _value), _on_token ? " TOKEN" : "");
}

static std::vector<const column_definition*> candidates_for_filtering_or_index(const schema& schema, const std::vector<restriction>& restrictions) {
    std::vector<const column_definition*> candidates;
    // 1. Assume only single, for simplicity now

    auto get_dependent_restrictions = [&restrictions](const column_definition& cdef) {
        return restrictions | boost::adaptors::filtered([&cdef] (const restriction& restr) {
            return boost::find_if(restr._target, [&cdef] (const column_definition* target_col) {
                return target_col == &cdef;
            }) != restr._target.end();
        });
    };

    bool is_key_prefix = true;
    for (const column_definition& cdef : schema.partition_key_columns()) {
        // I know, O(n^2), but n is bounded byt he number of restrictions, which is really low.
        // TODO(sarna): sort restrictions by position of cdef in schema
        auto dependent_restrictions = get_dependent_restrictions(cdef);
        rlogger.warn("Dependent restrictions empty0? {}", dependent_restrictions.empty());
        if (dependent_restrictions.empty()) {
            is_key_prefix = false;
            continue;
        }
        const restriction& restr = dependent_restrictions.front();
        if (restr._op != operator_type::EQ) {
            is_key_prefix = false;
        }
        if (std::next(dependent_restrictions.begin()) != dependent_restrictions.end()) {
            is_key_prefix = false;
        }
        if (!is_key_prefix) {
            rlogger.warn("ADDING1 {}", cdef.name_as_text());
            candidates.push_back(&cdef);
        }
    }

    bool is_eq_only = true;
    for (const column_definition& cdef : schema.clustering_key_columns()) {
        //TODO(sarna): deduplicate
        auto dependent_restrictions = get_dependent_restrictions(cdef);
        rlogger.warn("Dependent restrictions empty1? {}", dependent_restrictions.empty());
        if (dependent_restrictions.empty()) {
            is_key_prefix = false;
            continue;
        }
        for (const restriction& restr : dependent_restrictions) {
            if (restr._op == operator_type::LT || restr._op == operator_type::LTE || restr._op == operator_type::GT || restr._op == operator_type::GTE) { //FIXME(sarna): check that it's <=, <, >, >=
                if (!is_eq_only) {
                    is_key_prefix = false;
                }
                is_eq_only = false;
            } else if (restr._op != operator_type::EQ && restr._op != operator_type::IN) {
                is_key_prefix = false;
            }
        }
        if (!is_key_prefix) {
            rlogger.warn("ADDING2 {}", cdef.name_as_text());
            candidates.push_back(&cdef);
        }
    }

    for (const column_definition& cdef : schema.regular_columns()) {
        auto dependent_restrictions = get_dependent_restrictions(cdef);
        rlogger.warn("Dependent restrictions empty? {}", dependent_restrictions.empty());
        if (!dependent_restrictions.empty()) {
            rlogger.warn("ADDING3 {}", cdef.name_as_text());
            candidates.push_back(&cdef);
        }
    }

    return candidates;
}

prepared_restrictions prepared_restrictions::prepare_restrictions(database& db, const schema& schema, const std::vector<::shared_ptr<relation>>& where_clause, ::shared_ptr<variable_specifications> bound_names) {
    prepared_restrictions prepared;

    //FIXME(sarna): We need to collect marker specifications too: term->collect_marker_specification(bound_names),
    // or decide to do it later, which is also fine.

    auto transform_to_cdef = [&schema] (shared_ptr<column_identifier::raw> raw_ident) -> const column_definition* {
        schema_ptr s = schema.shared_from_this();
        auto ident = raw_ident->prepare_column_identifier(s);
        return get_column_definition(s, *ident);
    };

    for (auto&& rel : where_clause) {
        const operator_type& op = rel->get_operator();
        restriction restr(op);

        if (rel->is_multi_column()) {
            ::shared_ptr<multi_column_relation> multi_rel = static_pointer_cast<multi_column_relation>(rel);

            auto cdefs = multi_rel->get_entities() | boost::adaptors::transformed(transform_to_cdef);
            restr._target = boost::copy_range<std::vector<const column_definition*>>(cdefs);

            auto specs = boost::copy_range<std::vector<::shared_ptr<column_specification>>>(cdefs | boost::adaptors::transformed([&schema] (const column_definition* cdef) {
                return schema.make_column_specification(*cdef);
            }));

            auto raw_value = multi_rel->get_value();
            if (raw_value) {
                restr._value = raw_value->prepare(db, schema.ks_name(), specs);
            } else {
                restr._value = boost::copy_range<std::vector<::shared_ptr<term>>>(multi_rel->get_in_values() | boost::adaptors::transformed([&db, &schema, &specs] (::shared_ptr<term::multi_column_raw> raw_term) -> ::shared_ptr<term> {
                    return raw_term->prepare(db, schema.ks_name(), specs);
                }));
            }
        } else if (rel->on_token()) {
            ::shared_ptr<token_relation> token_rel = static_pointer_cast<token_relation>(rel);

            auto cdefs = token_rel->get_entities() | boost::adaptors::transformed(transform_to_cdef);
            restr._target = boost::copy_range<std::vector<const column_definition*>>(cdefs);

            restr._value = token_rel->get_value()->prepare(db, schema.ks_name(), schema.make_column_specification(*cdefs.front()));
            restr._on_token = true;
        } else {
            ::shared_ptr<single_column_relation> single_rel = static_pointer_cast<single_column_relation>(rel);

            const column_definition* cdef = transform_to_cdef(single_rel->get_entity());
            restr._target = std::vector<const column_definition*>{cdef};

            auto raw_value = single_rel->get_value();
            auto raw_map_key = single_rel->get_map_key();
            auto spec = schema.make_column_specification(*cdef);
            if (raw_map_key) {
                restr._value = map_entry{raw_map_key->prepare(db, schema.ks_name(), spec), raw_value->prepare(db, schema.ks_name(), spec)};
            } else if (raw_value) {
                restr._value = raw_value->prepare(db, schema.ks_name(), spec);
            } else {
                // IN with values
                restr._value = boost::copy_range<std::vector<::shared_ptr<term>>>(single_rel->get_in_values() | boost::adaptors::transformed([&db, &schema, &spec] (::shared_ptr<term::raw> raw_term) -> ::shared_ptr<term> {
                    return raw_term->prepare(db, schema.ks_name(), spec);
                }));
            }
        }
        prepared._restrictions.push_back(std::move(restr));
    }

    auto candidates = candidates_for_filtering_or_index(schema, prepared._restrictions);

    if (candidates.size() > 1) {
        rlogger.warn("More than 1 candidate ({}) - we won't cover it with indexes anyway - filtering is needed", candidates.size());
    }
    for (const column_definition* candidate : candidates) {
        rlogger.warn("SARNA candidate: {}", candidate->name_as_text());
        rlogger.warn("Let's check if there's a backing index for it, so we can score it. "
                "For every index, check if it can be applied and score it if it can. (Index, all restrictions) -> score. We need to get index target to see its pk/ck and see what matches. "
                "Index should return the columns it depends on - targets + if something is computed, then that too. "
                "If an index uses a longer key prefix, then it's more concise and should have higher score (prefix length is a good factor).");
    }

    return prepared;
}

}
