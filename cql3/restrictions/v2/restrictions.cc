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
#include "database.hh"
#include "cartesian_product.hh"
#include "types/map.hh"
#include "types/set.hh"

namespace cql3::restrictions::v2 {

static logging::logger rlogger("restrictions.v2");

sstring restriction::to_string() const {
    struct to_string_visitor {
        sstring operator()(const single_value& v) const {
            return "single_value: " + v->to_string();
        }
        sstring operator()(const multiple_values& vs) const {
            return "multiple_values: " + ::join(",", vs | boost::adaptors::transformed([] (::shared_ptr<term> single) { return single->to_string(); }));
        }
        sstring operator()(const map_entry& v) const {
            return format("map_entry: {{{},{}}}", v.key->to_string(), v.value->to_string());
        }
        sstring operator()(const term_slice& v) const {
            return "term_slice: " + v.to_string();
        }
        sstring operator()(const ::shared_ptr<abstract_marker>& v) const {
            return "abstract_marker: " + v->to_string();
        }
    };

    return format("{} {} {}{}", _op, ::join(",", _target | boost::adaptors::transformed([] (const column_definition* cdef) { return cdef->name_as_text(); })), std::visit(to_string_visitor(), _value), _on_token ? " TOKEN" : "");
}

bool restriction::depends_on(const column_definition& cdef) const {
    return boost::find(_target, &cdef) != _target.end();
}

bool restriction::depends_on_pk() const {
    return boost::algorithm::any_of(_target, [] (const column_definition* cdef) { return cdef->is_partition_key(); });
}

bool restriction::depends_on_ck() const {
    return boost::algorithm::any_of(_target, [] (const column_definition* cdef) { return cdef->is_clustering_key(); });
}

bool restriction::depends_on_regular_column() const {
    return boost::algorithm::any_of(_target, [] (const column_definition* cdef) { return cdef->is_regular(); });
}

static bool multi_column_restriction_is_prefix(const restriction& r, schema::const_iterator_range_type columns) {
    auto column_it = columns.begin();
    for (const column_definition* cdef : r._target) {
        if (cdef != &*column_it) {
            rlogger.warn("MULTI-COLUMN RESTRICTION IS NOT PREFIX");
            return false;
        }
        ++column_it;
    }
    rlogger.warn("Multi-column restriction is prefix");
    return true;
}

static std::vector<const column_definition*> candidates_for_filtering_or_index(const schema& schema, const std::vector<restriction>& restrictions) {
    std::vector<const column_definition*> candidates;

    auto get_dependent_restrictions = [&restrictions](const column_definition& cdef) {
        return restrictions | boost::adaptors::filtered([&cdef] (const restriction& restr) {
            return boost::find_if(restr._target, [&cdef] (const column_definition* target_col) {
                return target_col == &cdef;
            }) != restr._target.end();
        });
    };

    // Part 1: partition key
    auto pk_restrictions = restrictions | boost::adaptors::filtered([] (const restriction& r) { return r.depends_on_pk(); });
    const bool pk_on_token = boost::algorithm::all_of(pk_restrictions, [] (const restriction& r) { return r.on_token(); });
    const bool pk_has_unrestricted_components = std::distance(pk_restrictions.begin(), pk_restrictions.end()) < schema.partition_key_size();
    for (const restriction& r : pk_restrictions) {
        if (pk_has_unrestricted_components || (!r.on_token() && r._op != operator_type::EQ && r._op != operator_type::IN)) { //FIXME(sarna): May be more complicated for IN
            rlogger.warn("ADDING1 {}", r._target.front());
            candidates.push_back(r._target.front());
        }
    }
    const bool pk_needs_filtering = !candidates.empty();
    rlogger.warn("on token={}, has_unrestricted={}, needs_filtering={}", pk_on_token, pk_has_unrestricted_components, pk_needs_filtering);

    // Part 2: clustering key
    bool is_key_prefix = !pk_restrictions.empty() && !pk_needs_filtering; // NOTICE(sarna): if pk needs filtering, we definitely do not have a prefix
    const column_definition* prev_target = nullptr;
    bool is_eq_only = true;
    for (const column_definition& cdef : schema.clustering_key_columns()) {
        auto dependent_restrictions = get_dependent_restrictions(cdef);
        rlogger.warn("{}: Dependent restrictions {}: {}", cdef.name_as_text(), std::distance(dependent_restrictions.begin(), dependent_restrictions.end()), dependent_restrictions);
        if (dependent_restrictions.empty()) {
            rlogger.warn("depempty -> keyprefix false");
            is_key_prefix = false;
            continue;
        }
        //TODO(sarna): Let's ensure they are sorted wrt. columns
        for (const restriction& restr : dependent_restrictions) {
            if (restr._target.size() > 1) {
                rlogger.warn("FIXME(sarna): ADD SUPPORT FOR MULTI-COLUMN RESTRICTIONS WITH TUPLES AND ALL. SKIPPING FOR NOW");
                if (!multi_column_restriction_is_prefix(restr, schema.clustering_key_columns())) {
                    is_key_prefix = false;
                    break;
                } else {
                    continue;
                }
            }
            const column_definition* current_target = restr._target.front();
            rlogger.warn("OP == {}", restr._op.to_string());
            if (restr._op == operator_type::LT || restr._op == operator_type::LTE || restr._op == operator_type::GT || restr._op == operator_type::GTE) {
                if (!is_eq_only && prev_target != current_target) {
                    rlogger.warn("not eq only and targets are different -> keyprefix false");
                    is_key_prefix = false;
                    break;
                }
                is_eq_only = false;
            } else if (restr._op != operator_type::EQ && restr._op != operator_type::IN) {
                rlogger.warn("! eq and !in -> keyprefix false");
                is_key_prefix = false;
                break;
            }
            prev_target = current_target;
        }
        if (!is_key_prefix) {
            rlogger.warn("ADDING2 {}", cdef.name_as_text());
            candidates.push_back(&cdef);
        }
    }

    // Part 3: regular and static columns
    for (const column_definition& cdef : schema.regular_columns()) {
        auto dependent_restrictions = get_dependent_restrictions(cdef);
        rlogger.warn("Dependent restrictions: {}", dependent_restrictions);
        if (!dependent_restrictions.empty()) {
            rlogger.warn("ADDING3 {}", cdef.name_as_text());
            candidates.push_back(&cdef);
        }
    }
    for (const column_definition& cdef : schema.static_columns()) {
        auto dependent_restrictions = get_dependent_restrictions(cdef);
        rlogger.warn("Dependent restrictions: {}", dependent_restrictions);
        if (!dependent_restrictions.empty()) {
            rlogger.warn("ADDING4 {}", cdef.name_as_text());
            candidates.push_back(&cdef);
        }
    }

    return candidates;
}

static int score_index(const column_definition& candidate, const std::vector<restriction>& restrictions, const secondary_index::index& index) {
    sstring index_target_column = index.target_column();
    if (candidate.name_as_text() != index_target_column) {
        rlogger.warn("Names don't match: {} {}", candidate.name_as_text(), index_target_column);
        return 0;
    }
    if (index.metadata().local()) {
        const bool allow_local = boost::algorithm::all_of(restrictions, [] (const restriction& restr) {
            bool is_eq = restr._op == operator_type::EQ;
            return boost::algorithm::all_of(restr._target, [&is_eq] (const column_definition* cdef) {
                return !cdef->is_partition_key() || is_eq;
            });
        });
        rlogger.warn("Local index, so scoring {}", allow_local ? 2 : 0);
        return allow_local ? 2 : 0;
    }
    rlogger.warn("A regular index, so 1");
    return 1;
};

static std::pair<const column_definition*, std::optional<secondary_index::index>>
choose_index(const std::vector<const column_definition*>& candidates, const std::vector<restriction>& restrictions, const secondary_index::secondary_index_manager& sim) {
    for (const column_definition* candidate : candidates) {
        std::optional<secondary_index::index> chosen_index;
        int chosen_index_score = 0;
        for (const auto& index : sim.list_indexes()) {
            int current_score = score_index(*candidate, restrictions, index);
            if (current_score > chosen_index_score) {
                chosen_index = index;
                chosen_index_score = current_score;
                // FIXME(sarna): Let's keep dependent restrictions too
            }
        }
        return {candidate, chosen_index};
    }
    return {nullptr, {}};
}

struct collect_marker_visitor {
    ::shared_ptr<variable_specifications> bound_names;

    void operator()(single_value& v) const {
        rlogger.warn("MARKING SINGLE VALUE {}", v->to_string());
        return v->collect_marker_specification(bound_names);
    }
    void operator()(multiple_values& vs) const {
        for (const auto& v : vs) {
            rlogger.warn("MARKING MULTI VALUE {}", v->to_string());
            v->collect_marker_specification(bound_names);
        }
    }
    void operator()(map_entry& v) const {
        rlogger.warn("MARKING MAP VALUE {}", v.value->to_string());
        return v.value->collect_marker_specification(bound_names);
    }
    void operator()(term_slice& v) const {
        throw std::runtime_error("Cannot collect marker specification from a slice (yet)");
    }
    void operator()(::shared_ptr<abstract_marker>& v) const {
        rlogger.warn("MARKING MARKER {}", v->to_string());
        return v->collect_marker_specification(bound_names);
    }
};

prepared_restrictions prepared_restrictions::prepare_restrictions(database& db, schema_ptr schema, const std::vector<::shared_ptr<relation>>& where_clause, ::shared_ptr<variable_specifications> bound_names) {
    prepared_restrictions prepared(schema);

    //FIXME(sarna): We need to collect marker specifications too: term->collect_marker_specification(bound_names),
    // or decide to do it later, which is also fine.

    auto transform_to_cdef = [&schema] (shared_ptr<column_identifier::raw> raw_ident) -> const column_definition* {
        auto ident = raw_ident->prepare_column_identifier(schema);
        return get_column_definition(schema, *ident);
    };

    for (auto&& rel : where_clause) {
        const operator_type& op = rel->get_operator();

        if (op == operator_type::IS_NOT) {
            rlogger.warn("FIXME: IS_NOT is (almost) useless aside from screaming during CREATE MATERIALIZED VIEW that it's not here. Ignoring for now");
            continue;
        }

        restriction restr(op);

        if (rel->is_multi_column()) {
            rlogger.warn("RELATION TYPE: multi_column");
            ::shared_ptr<multi_column_relation> multi_rel = static_pointer_cast<multi_column_relation>(rel);

            auto cdefs = multi_rel->get_entities() | boost::adaptors::transformed(transform_to_cdef);
            restr._target = boost::copy_range<std::vector<const column_definition*>>(cdefs);

            auto specs = boost::copy_range<std::vector<::shared_ptr<column_specification>>>(cdefs | boost::adaptors::transformed([&schema] (const column_definition* cdef) {
                return schema->make_column_specification(*cdef);
            }));

            //FIXME(sarna): Unpack this tuple from a single term to a vector of terms
            auto raw_value = multi_rel->get_value();
            if (raw_value) {
                restr._value = raw_value->prepare(db, schema->ks_name(), specs);
            } else {
                restr._value = boost::copy_range<std::vector<::shared_ptr<term>>>(multi_rel->get_in_values() | boost::adaptors::transformed([&db, &schema, &specs] (::shared_ptr<term::multi_column_raw> raw_term) -> ::shared_ptr<term> {
                    return raw_term->prepare(db, schema->ks_name(), specs);
                }));
            }
        } else if (rel->on_token()) {
            rlogger.warn("RELATION TYPE: token");

            if (op == operator_type::LIKE) {
                throw exceptions::invalid_request_exception("LIKE cannot be used with the token function");
            }

            ::shared_ptr<token_relation> token_rel = static_pointer_cast<token_relation>(rel);

            auto cdefs = token_rel->get_entities() | boost::adaptors::transformed(transform_to_cdef);
            restr._target = boost::copy_range<std::vector<const column_definition*>>(cdefs);

            auto token_spec = ::make_shared<cql3::column_specification>("", "", ::make_shared<cql3::column_identifier>("", true), dht::global_partitioner().get_token_validator());
            restr._value = token_rel->get_value()->prepare(db, schema->ks_name(), token_spec);
            restr._on_token = true;
        } else {
            rlogger.warn("RELATION TYPE: single_column");
            ::shared_ptr<single_column_relation> single_rel = static_pointer_cast<single_column_relation>(rel);

            const column_definition* cdef = transform_to_cdef(single_rel->get_entity());
            restr._target = std::vector<const column_definition*>{cdef};

            auto raw_value = single_rel->get_value();
            auto raw_map_key = single_rel->get_map_key();
            auto id = ::make_shared<cql3::column_identifier>("", utf8_type);
            rlogger.warn("Original is {}. rmk {}, rw {}", cdef->type->name(), bool(raw_map_key), bool(raw_value));
            if (raw_map_key) {
                auto type = dynamic_pointer_cast<const map_type_impl>(cdef->type)->get_keys_type();
                rlogger.warn("TYPE IS {}", type ? type->name() : sstring("x"));
                auto spec = ::make_shared<cql3::column_specification>(schema->ks_name(), schema->cf_name(), std::move(id), type);
                restr._value = map_entry{raw_map_key->prepare(db, schema->ks_name(), spec), raw_value->prepare(db, schema->ks_name(), spec)};
            } else if (raw_value) {
                // FIXME(sarna): double-check if the types match for collections
                auto set_ptr = dynamic_pointer_cast<const set_type_impl>(cdef->type);
                auto collection_ptr = dynamic_pointer_cast<const collection_type_impl>(cdef->type);
                auto type = set_ptr ? set_ptr->get_elements_type() : collection_ptr ? collection_ptr->value_comparator() : cdef->type; // FIXME(sarna): hopefully, use visitors
                rlogger.warn("(collection_ptr? {}) TYPE2 IS {}", bool(collection_ptr), type ? type->name() : sstring("y"));
                auto spec = ::make_shared<cql3::column_specification>(schema->ks_name(), schema->cf_name(), std::move(id), type);
                restr._value = raw_value->prepare(db, schema->ks_name(), spec);
            } else {
                rlogger.warn("TYPE3 IS {}", cdef->type->name());
                auto spec = ::make_shared<cql3::column_specification>(schema->ks_name(), schema->cf_name(), std::move(id), cdef->type);
                // IN with values
                restr._value = boost::copy_range<std::vector<::shared_ptr<term>>>(single_rel->get_in_values() | boost::adaptors::transformed([&db, &schema, &spec] (::shared_ptr<term::raw> raw_term) -> ::shared_ptr<term> {
                    return raw_term->prepare(db, schema->ks_name(), spec);
                }));
            }
        }
        std::visit(collect_marker_visitor{bound_names}, restr._value);
        prepared._restrictions.push_back(std::move(restr));
    }

    auto candidates = candidates_for_filtering_or_index(*schema, prepared._restrictions);

    if (candidates.size() > 1) {
        rlogger.warn("More than 1 candidate ({}) - we won't cover it with indexes anyway - filtering is needed", candidates.size());
    }

    if (candidates.empty()) {
        //FIXME(sarna): more stuff to do here actually
        return prepared;
    }

    //FIXME(sarna): We're looking for the first candidate that has an index, not *the* first one.
    // Indexing and filtering can occur anyway
    for (const column_definition* candidate : candidates) {
        rlogger.warn("SARNA candidate: {}", candidate->name_as_text());
    }

    secondary_index::secondary_index_manager& sim = db.find_column_family(schema->id()).get_index_manager();
    auto [candidate, chosen_index] = choose_index(candidates, prepared._restrictions, sim);
    rlogger.warn("SARNA: Index choosing: {}", chosen_index ? chosen_index->metadata().name() : "<null>");

    prepared._filtered_columns = std::move(candidates);
    if (chosen_index) {
        prepared._filtered_columns.erase(boost::find(prepared._filtered_columns, candidate));
    }
    prepared._index = std::move(chosen_index);

    return prepared;
}

bool prepared_restrictions::need_filtering() const {
    return !_filtered_columns.empty();
}

bool prepared_restrictions::uses_indexing() const {
    return bool(_index);
}

bool prepared_restrictions::all_satisfy(std::function<bool(const restriction&)>&& cond) const {
    return boost::algorithm::all_of(_restrictions, cond);
}

bool prepared_restrictions::is_key_range() const {
    auto pk_restrs = pk_restrictions();
    rlogger.warn("DISTANCE {}", std::distance(pk_restrs.begin(), pk_restrs.end()));
    return boost::algorithm::any_of(pk_restrs, [&] (const restriction& r) {
        rlogger.warn("is on token? {}; needs filtering? {}", r.on_token(), needs_filtering(r));
        return r.on_token() || needs_filtering(r);
    }) || std::distance(pk_restrs.begin(), pk_restrs.end()) < _schema->partition_key_size();
}

static bytes_opt to_bytes_opt(const cql3::raw_value_view& view) {
    auto buffer_view = view.data();
    if (buffer_view) {
        return bytes_opt(linearized(*buffer_view));
    }
    return bytes_opt();
}

struct get_values_visitor {
    const query_options& _options;

    std::vector<bytes_opt> operator()(const single_value& v) const {
        rlogger.warn("GETTING SINGLE VALUE {}: {}", v, to_bytes_opt(v->bind_and_get(_options)));
        return {to_bytes_opt(v->bind_and_get(_options))};
    }

    std::vector<bytes_opt> operator()(const multiple_values& vs) const {
        return boost::copy_range<std::vector<bytes_opt>>(vs | boost::adaptors::transformed([this] (const single_value& v) {
            rlogger.warn("GETTING multi VALUE {}", v);
            return to_bytes_opt(v->bind_and_get(_options));
        }));
    }

    std::vector<bytes_opt> operator()(const term_slice& v) const {
        throw std::runtime_error("Cannot extract values from term slice");
    }

    std::vector<bytes_opt> operator()(const marker& m) const {
        rlogger.warn("GETTING marker {}", m);
        return {to_bytes_opt(m->bind_and_get(_options))};
    }

    std::vector<bytes_opt> operator()(const map_entry& m) const {
        rlogger.warn("GETTING map {}:{}", m.key, m.value);
        return {to_bytes_opt(m.value->bind_and_get(_options))};
    }
};

static std::vector<query::range<partition_key>> compute_pk_bounds(const prepared_restrictions& restrictions, const query_options& options) {
    using range_type = query::range<partition_key>;
    std::vector<range_type> ranges;

    const schema& schema = *restrictions._schema;

    auto invalid_null_msg = "Cannot compute bounds: no value in column {}";

    auto pk_restrictions = restrictions.pk_restrictions();
    assert(!pk_restrictions.empty());
    if (boost::algorithm::all_of(pk_restrictions, [] (const restriction& r) { return r._op == operator_type::EQ; })) {
        ranges.reserve(1);
        if (!pk_restrictions.empty() && std::next(pk_restrictions.begin()) == pk_restrictions.end()) {
            rlogger.warn("ONLY ONE!");
            auto&& e = pk_restrictions.front();
            const column_definition* cdef = e._target[0]; // FIXME(sarna): Assumes single column restrictions, true for pk, unsafe for future uses
            bytes_opt val = std::visit(get_values_visitor{options}, e._value)[0];
            if (!val) {
                throw exceptions::invalid_request_exception(sprint(invalid_null_msg, cdef->name_as_text()));
            }
            rlogger.warn("Got value {}", val);
            ranges.emplace_back(range_type::make_singular(partition_key::from_single_value(schema, std::move(*val))));
            return ranges;
        }
        std::vector<bytes> components;
        components.reserve(restrictions.size());
        for (auto&& e : restrictions._restrictions) {
            const column_definition* cdef = e._target[0]; // FIXME(sarna): Assumes single column restrictions, true for pk, unsafe for future uses
            rlogger.warn("Getting for {}", cdef->name_as_text());
            bytes_opt val = std::visit(get_values_visitor{options}, e._value)[0];
            if (!val) {
                throw exceptions::invalid_request_exception(sprint(invalid_null_msg, cdef->name_as_text()));
            }
            rlogger.warn("Got value2 {}", *val);
            components.emplace_back(std::move(*val));
        }
        ranges.emplace_back(range_type::make_singular(partition_key::from_exploded(schema, std::move(components))));
        rlogger.warn("New ranges: {}", ranges);
        return ranges;
    }

    std::vector<std::vector<bytes_opt>> vec_of_values;
    for (auto&& e : restrictions._restrictions) {
        const column_definition* def = e._target[0]; // FIXME(sarna): Assumes single column restrictions, true for pk, unsafe for future uses

        rlogger.warn("SARNA: TODO: resolve with another visitor part");
        /*TODO(sarna): resolve this - needs another visitor
         * if (vec_of_values.size() != _schema->position(*def) || r->is_contains()) {
            // The prefixes built so far are the longest we can build,
            // the rest of the constraints will have to be applied using filtering.
            break;
        }*/

        auto values = std::visit(get_values_visitor{options}, e._value);
        for (auto&& val : values) {
            if (!val) {
                throw exceptions::invalid_request_exception(sprint(invalid_null_msg, def->name_as_text()));
            }
        }
        if (values.empty()) {
            return {};
        }
        vec_of_values.emplace_back(std::move(values));
    }

    ranges.reserve(cartesian_product_size(vec_of_values));
    for (auto&& prefix : make_cartesian_product(vec_of_values)) {
        ranges.emplace_back(range_type::make_singular(partition_key::from_optional_exploded(schema, std::move(prefix))));
    }

    return std::move(ranges);
}

dht::partition_range_vector prepared_restrictions::get_partition_key_ranges(const query_options& options) const {
    if (pk_restrictions().empty()) {
        return {dht::partition_range::make_open_ended_both_sides()};
    }
    if (boost::algorithm::any_of(pk_restrictions(), [&] (const restriction& r) { return needs_filtering(r); })) {
        return {dht::partition_range::make_open_ended_both_sides()};
    }

    dht::partition_range_vector ranges;
    //return ranges; //FIXME(sarna): bind_and_get doesn't work, probably because of bound variables
    ranges.reserve(size());
    for (query::range<partition_key>& r : compute_pk_bounds(*this, options)) {
        if (!r.is_singular()) {
            throw exceptions::invalid_request_exception("v2: Range queries on partition key values not supported.");
        }
        ranges.emplace_back(std::move(r).transform(
            [this] (partition_key&& k) -> query::ring_position {
                auto token = dht::global_partitioner().get_token(*_schema, k);
                return { std::move(token), std::move(k) };
            }));
    }
    return ranges;
}

}
