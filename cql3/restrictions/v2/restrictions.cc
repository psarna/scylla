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

using pk_restrictions_range = typename std::result_of<decltype(&prepared_restrictions::pk_restrictions)(prepared_restrictions*)>::type;
using ck_restrictions_range = typename std::result_of<decltype(&prepared_restrictions::ck_restrictions)(prepared_restrictions*)>::type;

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
        //sstring operator()(const ::shared_ptr<abstract_marker>& v) const {
        //    return "abstract_marker: " + v->to_string();
        //}
    };

    return format("{} {} {}{}", *_op, ::join(",", _target | boost::adaptors::transformed([] (const column_definition* cdef) { return cdef->name_as_text(); })), std::visit(to_string_visitor(), _value), _on_token ? " TOKEN" : "");
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
        if (r.on_token()) {
            continue;
        }
        if (pk_has_unrestricted_components || (r._op != &operator_type::EQ && r._op != &operator_type::IN)) { //FIXME(sarna): May be more complicated for IN
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
            rlogger.warn("OP == {}", restr._op->to_string());
            if (restr._op == &operator_type::LT || restr._op == &operator_type::LTE || restr._op == &operator_type::GT || restr._op == &operator_type::GTE) {
                if (!is_eq_only && prev_target != current_target) {
                    rlogger.warn("not eq only and targets are different -> keyprefix false");
                    is_key_prefix = false;
                    break;
                }
                is_eq_only = false;
            } else if (restr._op != &operator_type::EQ && restr._op != &operator_type::IN) {
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

static int score_index(const column_definition& candidate, const std::vector<restriction>& restrictions, const secondary_index::index& index, bool allow_local) {
    sstring index_target_column = index.target_column();
    if (candidate.name_as_text() != index_target_column) {
        rlogger.warn("Names don't match: {} {}", candidate.name_as_text(), index_target_column);
        return 0;
    }
    if (index.metadata().local()) {
        rlogger.warn("Local index, so scoring {}", allow_local ? 2 : 0);
        return allow_local ? 2 : 0;
    }
    rlogger.warn("A regular index, so 1");
    return 1;
};

static std::pair<const column_definition*, std::optional<secondary_index::index>>
choose_index(const schema& schema, const std::vector<const column_definition*>& candidates, const std::vector<restriction>& restrictions, const secondary_index::secondary_index_manager& sim) {
    std::optional<secondary_index::index> chosen_index;
    const column_definition* chosen_candidate = nullptr;
    int chosen_index_score = 0;

    auto pk_restrictions = restrictions | boost::adaptors::filtered([&] (const restriction& r) { return r.depends_on_pk(); });
    const bool allow_local = boost::algorithm::all_of(pk_restrictions, [&] (const restriction& restr) {
        const bool is_eq = restr._op == &operator_type::EQ;
        return boost::algorithm::all_of(restr._target, [&is_eq] (const column_definition* cdef) {
            return !cdef->is_partition_key() || is_eq;
        });
    }) && std::distance(pk_restrictions.begin(), pk_restrictions.end()) == schema.partition_key_size();

    for (const column_definition* candidate : candidates) {
        for (const auto& index : sim.list_indexes()) {
            rlogger.warn("Checking index {}", index.metadata().name());
            int current_score = score_index(*candidate, restrictions, index, allow_local);
            if (current_score > chosen_index_score) {
                chosen_index = index;
                chosen_index_score = current_score;
                chosen_candidate = candidate;
                // FIXME(sarna): Let's keep dependent restrictions too
            }
        }
    }
    // FIXME(sarna): for backward compatibility, we should return the first index found in the above loop, local or not.
    // But, since the previous heuristics is really bad, especially if there are local indexes involved, it's hereby changed
    // and we always prefer a local index over a global one.
    return {chosen_candidate, chosen_index};
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
        auto start_ptr = v.bound(statements::bound::START);
        auto end_ptr = v.bound(statements::bound::END);
        if (start_ptr) {
            start_ptr->collect_marker_specification(bound_names);
        }
        if (end_ptr) {
            end_ptr->collect_marker_specification(bound_names);
        }
    }
    //void operator()(::shared_ptr<abstract_marker>& v) const {
    //    rlogger.warn("MARKING MARKER {}", v->to_string());
    //    return v->collect_marker_specification(bound_names);
    //}
};

prepared_restrictions prepared_restrictions::prepare_restrictions(database& db, schema_ptr schema, const std::vector<::shared_ptr<relation>>& where_clause, ::shared_ptr<variable_specifications> bound_names) {
    prepared_restrictions prepared(schema);

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

            //FIXME(sarna): same on non-restr
            auto token_spec = ::make_shared<cql3::column_specification>("", "", ::make_shared<cql3::column_identifier>("", true), dht::global_partitioner().get_token_validator());
            auto val = token_rel->get_value()->prepare(db, schema->ks_name(), token_spec);
            if (op == operator_type::GT) {
                restr._value = term_slice{val, false, {}, false};
            } else if (op == operator_type::GTE) {
                restr._value = term_slice{val, true, {}, false};
            } else if (op == operator_type::LT) {
                restr._value = term_slice{{}, false, val, false};
            } else if (op == operator_type::LTE) {
                restr._value = term_slice{{}, false, val, true};
            } else {
                if (op != operator_type::EQ) {
                    throw std::runtime_error("FIXME(sarna): token restriction is neither = nor =/=");
                }
                restr._value = std::move(val);
            }
            restr._on_token = true;
        } else {
            rlogger.warn("RELATION TYPE: single_column");
            ::shared_ptr<single_column_relation> single_rel = static_pointer_cast<single_column_relation>(rel);

            const column_definition* cdef = transform_to_cdef(single_rel->get_entity());
            rlogger.warn("RELATION FOR {}", cdef->name_as_text());
            restr._target = std::vector<const column_definition*>{cdef};

            auto raw_value = single_rel->get_value();
            auto raw_map_key = single_rel->get_map_key();
            auto id = ::make_shared<cql3::column_identifier>("", utf8_type);
            rlogger.warn("Original is {}. rmk {}, rv {}", cdef->type->name(), bool(raw_map_key), bool(raw_value));
            if (raw_map_key) {
                auto map_type = dynamic_pointer_cast<const map_type_impl>(cdef->type);
                if (!map_type->is_multi_cell()) {
                    throw exceptions::invalid_request_exception(format("Map-entry equality predicates on frozen map column {} are not supported", cdef->name_as_text()));
                }
                rlogger.warn("TYPE IS {}", map_type ? map_type->name() : sstring("x"));
                auto key_spec = ::make_shared<cql3::column_specification>(schema->ks_name(), schema->cf_name(), id, map_type->get_keys_type());
                auto value_spec = ::make_shared<cql3::column_specification>(schema->ks_name(), schema->cf_name(), id, map_type->get_values_type());
                restr._value = map_entry{raw_map_key->prepare(db, schema->ks_name(), key_spec), raw_value->prepare(db, schema->ks_name(), value_spec)};
            } else if (raw_value) {
                // FIXME(sarna): double-check if the types match for collections
                auto maybe_get_element_type = [&] (data_type t) -> data_type {
                    if (op == operator_type::CONTAINS || op == operator_type::CONTAINS_KEY) {
                        auto set_ptr = dynamic_pointer_cast<const set_type_impl>(cdef->type);
                        auto collection_ptr = dynamic_pointer_cast<const collection_type_impl>(cdef->type);
                        rlogger.warn("(collection_ptr? {}) TYPE2 IS {}", bool(collection_ptr), t ? t->name() : sstring("y"));
                        return set_ptr ? set_ptr->get_elements_type() : collection_ptr ? collection_ptr->value_comparator() : t;
                    } else {
                        rlogger.warn("returning just type");
                        return cdef->type;
                    }
                };
                auto type = maybe_get_element_type(cdef->type);
                auto spec = ::make_shared<cql3::column_specification>(schema->ks_name(), schema->cf_name(), std::move(id), type);
                rlogger.warn("Will prepare _value");
                single_value val = raw_value->prepare(db, schema->ks_name(), spec);
                // FIXME: copy paste from token code - should be a helper function
                if (op == operator_type::GT) {
                    restr._value = term_slice{val, false, {}, false};
                } else if (op == operator_type::GTE) {
                    restr._value = term_slice{val, true, {}, false};
                } else if (op == operator_type::LT) {
                    restr._value = term_slice{{}, false, val, false};
                } else if (op == operator_type::LTE) {
                    restr._value = term_slice{{}, false, val, true};
                } else {
                    restr._value = std::move(val);
                }
            } else {
                rlogger.warn("TYPE3 IS {}", cdef->type->name());
                auto spec = ::make_shared<cql3::column_specification>(schema->ks_name(), schema->cf_name(), std::move(id), cdef->type);
                // IN with values
                restr._value = boost::copy_range<std::vector<::shared_ptr<term>>>(single_rel->get_in_values() | boost::adaptors::transformed([&db, &schema, &spec] (::shared_ptr<term::raw> raw_term) -> ::shared_ptr<term> {
                    return raw_term->prepare(db, schema->ks_name(), spec);
                }));
            }
        }
        rlogger.warn("Will visit");
        std::visit(collect_marker_visitor{bound_names}, restr._value);
        rlogger.warn("Visited");
        prepared._restrictions.push_back(std::move(restr));
    }

    // Sort the elements by id, which is incredibly useful later
    std::sort(prepared._restrictions.begin(), prepared._restrictions.end(), [] (const restriction& r1, const restriction& r2) {
        return r1._target[0]->id < r2._target[0]->id;
    });

    // Collapse all slices into single instances
    std::vector<restriction> restrictions_with_deduplicated_slices;
    restrictions_with_deduplicated_slices.reserve(prepared._restrictions.size());
    restriction* previous_slice;
    for (restriction& r : prepared._restrictions) {
        if (previous_slice && previous_slice->_target == r._target) {
            if (term_slice* slice = std::get_if<term_slice>(&r._value)) {
                term_slice* prev = std::get_if<term_slice>(&previous_slice->_value);
                prev->merge(*slice);
                continue;
            }
        }
        restrictions_with_deduplicated_slices.push_back(std::move(r));
        previous_slice = &restrictions_with_deduplicated_slices.back();
    }
    prepared._restrictions = std::move(restrictions_with_deduplicated_slices);

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
    auto [candidate, chosen_index] = choose_index(*schema, candidates, prepared._restrictions, sim);
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
        //FIXME(sarna): Marker should be a separate variant, not a special case for a single_value
        //FIXME(sarna): we can use "in_value" here, from tuples.hh, it has ->get_split_values()
        if (auto marker = dynamic_pointer_cast<abstract_marker>(v)) {
            auto terminal = marker->bind(_options);
            if (auto multi_item = dynamic_pointer_cast<multi_item_terminal>(terminal)) {
                //FIXME(sarna): If it's anything else than an IN operator, we shouldn't sort and erase
                auto values = multi_item->get_elements();
                std::sort(values.begin(), values.end());
                values.erase(std::unique(values.begin(), values.end()), values.end());
                rlogger.warn("RETURNING {}", values);
                return values;
            }
            return {terminal->get(_options).data()};
        }
        // FIXME(sarna): Remove copypasta
        if (auto multi_item = dynamic_pointer_cast<multi_item_terminal>(v->bind(_options))) {
            auto values = multi_item->get_elements();
            rlogger.warn("RETURNING {}", values);
            return values;
        }
        return {to_bytes_opt(v->bind_and_get(_options))};
    }

    std::vector<bytes_opt> operator()(const multiple_values& vs) const {
        auto values = boost::copy_range<std::vector<bytes_opt>>(vs | boost::adaptors::transformed([this] (const single_value& v) {
            rlogger.warn("GETTING multi VALUE {}. Serialized {}", v, to_bytes_opt(v->bind_and_get(_options)));
            return to_bytes_opt(v->bind_and_get(_options));
        }));
        //FIXME(sarna): If it's anything else than an IN operator, we shouldn't sort and erase
        std::sort(values.begin(), values.end());
        values.erase(std::unique(values.begin(), values.end()), values.end());
        rlogger.warn("RETURNING {}", values);
        return values;
    }

    std::vector<bytes_opt> operator()(const term_slice& v) const {
        throw std::runtime_error("Cannot extract values from term slice");
    }

    //std::vector<bytes_opt> operator()(const marker& m) const {
    //    rlogger.warn("GETTING marker {}", m);
    //    return {to_bytes_opt(m->bind_and_get(_options))};
    //}

    std::vector<bytes_opt> operator()(const map_entry& m) const {
        rlogger.warn("GETTING map {}:{}", m.key, m.value);
        return {to_bytes_opt(m.value->bind_and_get(_options))};
    }
};

static std::vector<dht::partition_range> bounds_token_ranges(const term_slice& token_slice, const query_options& options) {
    auto get_token_bound = [&] (statements::bound b) {
        if (!token_slice.has_bound(b)) {
            return is_start(b) ? dht::minimum_token() : dht::maximum_token();
        }
        auto buf = to_bytes_opt(token_slice.bound(b)->bind_and_get(options));
        if (!buf) {
            throw exceptions::invalid_request_exception("Invalid null token value");
        }
        auto tk = dht::global_partitioner().from_bytes(*buf);
        if (tk.is_minimum() && !is_start(b)) {
            // The token was parsed as a minimum marker (token::kind::before_all_keys), but
            // as it appears in the end bound position, it is actually the maximum marker
            // (token::kind::after_all_keys).
            return dht::maximum_token();
        }
        return tk;
    };

    const auto start_token = get_token_bound(statements::bound::START);
    const auto end_token = get_token_bound(statements::bound::END);
    const auto include_start = token_slice.is_inclusive(statements::bound::START);
    const auto include_end = token_slice.is_inclusive(statements::bound::END);

    /*
     * If we ask SP.getRangeSlice() for (token(200), token(200)], it will happily return the whole ring.
     * However, wrapping range doesn't really make sense for CQL, and we want to return an empty result in that
     * case (CASSANDRA-5573). So special case to create a range that is guaranteed to be empty.
     *
     * In practice, we want to return an empty result set if either startToken > endToken, or both are equal but
     * one of the bound is excluded (since [a, a] can contains something, but not (a, a], [a, a) or (a, a)).
     */
    if (start_token > end_token || (start_token == end_token && (!include_start || !include_end))) {
        return {};
    }

    typedef typename dht::partition_range::bound bound;

    auto start = bound(include_start
                       ? dht::ring_position::starting_at(start_token)
                       : dht::ring_position::ending_at(start_token));
    auto end = bound(include_end
                       ? dht::ring_position::ending_at(end_token)
                       : dht::ring_position::starting_at(end_token));

    return { dht::partition_range(std::move(start), std::move(end)) };
}

template<typename KeyType>
//FIXME(concept)
static std::vector<query::range<KeyType>> compute_single_column_bounds_from_slice(const column_definition* def,
        const term_slice* slice, const std::vector<std::vector<bytes_opt>>& vec_of_values, const schema& schema, const query_options& options) {
    using range_type = query::range<KeyType>;
    using range_bound = typename range_type::bound;
    std::vector<range_type> ranges;

    if (cartesian_product_is_empty(vec_of_values)) {
        auto read_bound = [slice, &options, &schema] (statements::bound b) -> std::optional<range_bound> {
            if (!slice->has_bound(b)) {
                return {};
            }
            auto value = to_bytes_opt(slice->bound(b)->bind_and_get(options));
            if (!value) {
                throw exceptions::invalid_request_exception(format("Cannot compute bounds: no value in column {}", slice->to_string()));
            }
            rlogger.warn("Ret {}", KeyType::from_single_value(schema, *value), slice->is_inclusive(b));
            return {range_bound(KeyType::from_single_value(schema, *value), slice->is_inclusive(b))};
        };
        ranges.emplace_back(range_type(read_bound(statements::bound::START), read_bound(statements::bound::END)));
        if (def->type->is_reversed()) {
            ranges.back().reverse();
        }
        rlogger.warn("Ranges {}", ranges);
        return ranges;
    }

    ranges.reserve(cartesian_product_size(vec_of_values));
    for (auto&& prefix : make_cartesian_product(vec_of_values)) {
        auto read_bound = [slice, &prefix, &options, &schema](statements::bound bound) -> range_bound {
            if (slice->has_bound(bound)) {
                auto value = to_bytes_opt(slice->bound(bound)->bind_and_get(options));
                if (!value) {
                    throw exceptions::invalid_request_exception(format("Cannot compute bounds: no value in column {}", slice->to_string()));
                }
                prefix.emplace_back(std::move(value));
                auto val = KeyType::from_optional_exploded(schema, prefix);
                prefix.pop_back();
                rlogger.warn("Reet {}", std::move(val), slice->is_inclusive(bound));
                return range_bound(std::move(val), slice->is_inclusive(bound));
            } else {
                rlogger.warn("Reeeeet {}", KeyType::from_optional_exploded(schema, prefix));
                return range_bound(KeyType::from_optional_exploded(schema, prefix));
            }
        };

        ranges.emplace_back(range_type(
            read_bound(statements::bound::START),
            read_bound(statements::bound::END)));

        if (def->type->is_reversed()) {
            ranges.back().reverse();
        }
    }

    rlogger.warn("From slice returning {}", ranges);
    return ranges;
}

static std::vector<query::range<clustering_key>> compute_single_column_ck_bounds(const schema& schema, ck_restrictions_range ck_restrictions, const query_options& options) {
    using range_type = query::range<clustering_key>;
    std::vector<range_type> ranges;

    assert(!ck_restrictions.empty());
    if (boost::algorithm::all_of(ck_restrictions, [] (const restriction& r) { return r._op == &operator_type::EQ; })) {
        ranges.reserve(1);
        if (!ck_restrictions.empty() && std::next(ck_restrictions.begin()) == ck_restrictions.end()) {
            rlogger.warn("ONLY ONE!");
            auto&& r = ck_restrictions.front();
            const column_definition* cdef = r._target[0]; // FIXME(sarna): Assumes single column restrictions
            bytes_opt val = std::visit(get_values_visitor{options}, r._value)[0];
            if (!val) {
                throw exceptions::invalid_request_exception(format("Cannot compute bounds: no value in column {}", cdef->name_as_text()));
            }
            rlogger.warn("Got value {}", val);
            ranges.emplace_back(range_type::make_singular(clustering_key::from_single_value(schema, std::move(*val))));
            return ranges;
        }
        std::vector<bytes> components;
        components.reserve(boost::size(ck_restrictions));
        for (auto&& r : ck_restrictions) {
            const column_definition* cdef = r._target[0]; // FIXME(sarna): Assumes single column restrictions
            rlogger.warn("Getting for {}", cdef->name_as_text());
            bytes_opt val = std::visit(get_values_visitor{options}, r._value)[0];
            if (!val) {
                throw exceptions::invalid_request_exception(format("Cannot compute bounds: no value in column {}", cdef->name_as_text()));
            }
            rlogger.warn("Got value2 {}", *val);
            components.emplace_back(std::move(*val));
        }
        ranges.emplace_back(range_type::make_singular(clustering_key::from_exploded(schema, std::move(components))));
        rlogger.warn("New ranges: {}", ranges);
        return ranges;
    }

    std::vector<std::vector<bytes_opt>> vec_of_values;
    for (const restriction& r : ck_restrictions) {
        const column_definition* def = r._target[0];

         if (vec_of_values.size() != schema.position(*def) || r._op == &operator_type::CONTAINS) {
            // The prefixes built so far are the longest we can build,
            // the rest of the constraints will have to be applied using filtering.
            break;
        }

        const term_slice* slice = std::get_if<term_slice>(&r._value);
        rlogger.warn("Slice? {} index={} op {}", bool(slice), r._value.index(), r._op->to_string());
        if (slice) {
            rlogger.warn("Will return from slice.");
            return compute_single_column_bounds_from_slice<clustering_key>(def, slice, vec_of_values, schema, options);
        }

        auto values = std::visit(get_values_visitor{options}, r._value);
        for (auto&& val : values) {
            if (!val) {
                throw exceptions::invalid_request_exception(format("Cannot compute bounds: no value in column {}", def->name_as_text()));
            }
        }
        if (values.empty()) {
            return {};
        }
        rlogger.warn("Emplacing {}", values);
        vec_of_values.emplace_back(std::move(values));
        rlogger.warn("ENDED EMPLACING");
    }

    rlogger.warn("vv ck RANGES IS {}", vec_of_values);
    ranges.reserve(cartesian_product_size(vec_of_values));
    for (auto&& prefix : make_cartesian_product(vec_of_values)) {
        rlogger.warn("Emplacing {}", range_type::make_singular(clustering_key::from_optional_exploded(schema, std::move(prefix))));
        ranges.emplace_back(range_type::make_singular(clustering_key::from_optional_exploded(schema, std::move(prefix))));
    }

    rlogger.warn("Returning cranges of {}", ranges);
    return ranges;
}

static std::vector<query::range<partition_key>> compute_single_column_pk_bounds(const schema& schema, pk_restrictions_range pk_restrictions, const query_options& options) {
    using range_type = query::range<partition_key>;
    std::vector<range_type> ranges;

    assert(!pk_restrictions.empty());
    if (boost::algorithm::all_of(pk_restrictions, [] (const restriction& r) { return !r.on_token() && r._op == &operator_type::EQ; })) {
        ranges.reserve(1);
        if (!pk_restrictions.empty() && std::next(pk_restrictions.begin()) == pk_restrictions.end()) {
            rlogger.warn("ONLY ONE!");
            auto&& r = pk_restrictions.front();
            const column_definition* cdef = r._target[0]; // FIXME(sarna): Assumes single column restrictions
            bytes_opt val = std::visit(get_values_visitor{options}, r._value)[0];
            if (!val) {
                throw exceptions::invalid_request_exception(format("Cannot compute bounds: no value in column {}", cdef->name_as_text()));
            }
            rlogger.warn("Got value {}", val);
            ranges.emplace_back(range_type::make_singular(partition_key::from_single_value(schema, std::move(*val))));
            return ranges;
        }
        std::vector<bytes> components;
        components.reserve(boost::size(pk_restrictions));
        for (auto&& r : pk_restrictions) {
            const column_definition* cdef = r._target[0]; // FIXME(sarna): Assumes single column restrictions
            rlogger.warn("Getting for {}", cdef->name_as_text());
            bytes_opt val = std::visit(get_values_visitor{options}, r._value)[0];
            if (!val) {
                throw exceptions::invalid_request_exception(format("Cannot compute bounds: no value in column {}", cdef->name_as_text()));
            }
            rlogger.warn("Got value2 {}", *val);
            components.emplace_back(std::move(*val));
        }
        ranges.emplace_back(range_type::make_singular(partition_key::from_exploded(schema, std::move(components))));
        rlogger.warn("New ranges: {}", ranges);
        return ranges;
    }

    std::vector<std::vector<bytes_opt>> vec_of_values;
    for (const restriction& r : pk_restrictions | boost::adaptors::filtered([] (const restriction& r) { return !r.on_token(); })) {
        const column_definition* def = r._target[0];

         if (vec_of_values.size() != schema.position(*def) || r._op == &operator_type::CONTAINS) {
            // The prefixes built so far are the longest we can build,
            // the rest of the constraints will have to be applied using filtering.
            break;
        }

        const term_slice* slice = std::get_if<term_slice>(&r._value);
        if (slice) {
            rlogger.warn("Will return from slice.");
            return compute_single_column_bounds_from_slice<partition_key>(def, slice, vec_of_values, schema, options);
        }

        auto values = std::visit(get_values_visitor{options}, r._value);
        for (auto&& val : values) {
            if (!val) {
                throw exceptions::invalid_request_exception(format("Cannot compute bounds: no value in column {}", def->name_as_text()));
            }
        }
        if (values.empty()) {
            return {};
        }
        rlogger.warn("Emplacing {}", values);
        vec_of_values.emplace_back(std::move(values));
        rlogger.warn("ENDED EMPLACING");
    }

    rlogger.warn("vv RANGES IS {}", vec_of_values);
    ranges.reserve(cartesian_product_size(vec_of_values));
    for (auto&& prefix : make_cartesian_product(vec_of_values)) {
        rlogger.warn("Emplacing {}", range_type::make_singular(partition_key::from_optional_exploded(schema, std::move(prefix))));
        ranges.emplace_back(range_type::make_singular(partition_key::from_optional_exploded(schema, std::move(prefix))));
    }

    rlogger.warn("Returning ranges of {}", ranges);
    return ranges;
}

dht::partition_range_vector prepared_restrictions::get_partition_key_ranges(const query_options& options) const {
    auto pk_restrictions = this->pk_restrictions();
    if (pk_restrictions.empty()) {
        return {dht::partition_range::make_open_ended_both_sides()};
    }
    if (boost::algorithm::any_of(pk_restrictions, [&] (const restriction& r) { return needs_filtering(r); })) {
        return {dht::partition_range::make_open_ended_both_sides()};
    }

    dht::partition_range_vector ranges;
    ranges.reserve(size());
    // Token slice needs to be merged into a single term slice
    auto token_restrictions = pk_restrictions | boost::adaptors::filtered([] (const restriction& r) { return r.on_token(); });
    std::unique_ptr<term_slice> token_slice;
    for (const restriction& token_restr : token_restrictions) {
        const term_slice* slice = std::get_if<term_slice>(&token_restr._value);
        if (slice) {
            if (!token_slice) {
                token_slice = std::make_unique<term_slice>(slice->bound(statements::bound::START), slice->is_inclusive(statements::bound::START),
                        slice->bound(statements::bound::END), slice->is_inclusive(statements::bound::END));
            } else {
                token_slice->merge(*slice);
            }
        }
    }
    if (token_slice) {
        rlogger.warn("Slice {}", *token_slice);
        auto token_ranges = bounds_token_ranges(*token_slice, options);
        ranges.insert(ranges.end(), token_ranges.begin(), token_ranges.end());
    } else if (!token_restrictions.empty()) {
        //FIXME: There must be no more than 1 EQ restriction if there were no slices. Throw instead of assert
        assert(std::next(token_restrictions.begin()) == token_restrictions.end());
        const restriction& token_restr = token_restrictions.front();
        bytes token_value = std::visit(get_values_visitor{options}, token_restr._value)[0].value();
        dht::token token = dht::global_partitioner().from_bytes(token_value);
        ranges.emplace_back(query::ring_position::starting_at(token), query::ring_position::ending_at(token));
    } else {
        for (query::range<partition_key>& r : compute_single_column_pk_bounds(*_schema, pk_restrictions, options)) {
            if (!r.is_singular()) {
                throw exceptions::invalid_request_exception("v2: Range queries on partition key values not supported.");
            }
            ranges.emplace_back(std::move(r).transform(
                [this] (partition_key&& k) -> query::ring_position {
                    auto token = dht::global_partitioner().get_token(*_schema, k);
                    return { std::move(token), std::move(k) };
                }));
        }
    }
    return ranges;
}

query::clustering_row_ranges prepared_restrictions::get_clustering_bounds(const query_options& options) const {
    auto ck_restrictions = this->ck_restrictions();
    if (ck_restrictions.empty()) {
        return {query::clustering_range::make_open_ended_both_sides()};
    }
    if (boost::algorithm::any_of(ck_restrictions, [&] (const restriction& r) { return needs_filtering(r); })) {
        //FIXME(sarna): Add longest prefix optimization here. Then we return bounds_ranges() of a longest prefix, like before:
        //if (auto single_ck_restrictions = dynamic_pointer_cast<single_column_clustering_key_restrictions>(_clustering_columns_restrictions)) {
        //    return single_ck_restrictions->get_longest_prefix_restrictions()->bounds_ranges(options);
        //}
        return {query::clustering_range::make_open_ended_both_sides()};
    }

    if (boost::algorithm::any_of(ck_restrictions, [&] (const restriction& r) { return r._target.size() > 1; })) {
        // Serve multi-column here. FIXME: slices, IN, etc. Assumes that we have *only* multi-column restrictions,
        // since CQL screams on mixing single and multi-column ones
        for (const restriction& r : ck_restrictions) {
            rlogger.warn("SERVING MULTI-COLUMN");
            if (r._op == &operator_type::EQ) {
                auto components = std::visit(get_values_visitor{options}, r._value);
                return {query::clustering_range::make_singular(clustering_key::from_optional_exploded(*_schema, std::move(components)))};
            } else if (r._op == &operator_type::IN) {
                // FIXME(sarna): in with values, in with marker, blah blah

                std::vector<std::vector<bytes_opt>> values;
                const multiple_values* multiple_vals = std::get_if<multiple_values>(&r._value);
                assert(multiple_vals); // FIXME: throw
                auto split_in_values = boost::copy_range<std::vector<std::vector<bytes_opt>>>(*multiple_vals | boost::adaptors::transformed([&] (::shared_ptr<term> t) {
                    return get_values_visitor{options}(t);
                }));

                std::vector<query::clustering_range> bounds;
                for (auto&& components : split_in_values) {
                    for (unsigned i = 0; i < components.size(); i++) {
                        statements::request_validations::check_not_null(components[i], "Invalid null value in condition for column %s", r._target.at(i)->name_as_text());
                    }
                    auto prefix = clustering_key_prefix::from_optional_exploded(*_schema, components);
                    bounds.emplace_back(query::clustering_range::make_singular(prefix));
                }
                auto less_cmp = clustering_key_prefix::less_compare(*_schema);
                std::sort(bounds.begin(), bounds.end(), [&] (query::clustering_range& x, query::clustering_range& y) {
                    return less_cmp(x.start()->value(), y.start()->value());
                });
                auto eq_cmp = clustering_key_prefix::equality(*_schema);
                bounds.erase(std::unique(bounds.begin(), bounds.end(), [&] (query::clustering_range& x, query::clustering_range& y) {
                    return eq_cmp(x.start()->value(), y.start()->value());
                }), bounds.end());
                return bounds;
            } else {
                throw std::runtime_error(format("NOT IMPLEMENTED YET :( {}", r._op->to_string()));
            }
        }
    }

    // These work for single_column_restrictions only. Multi-columns have an implementation in multi_column_restriction.hh
    auto wrapping_bounds = compute_single_column_ck_bounds(*_schema, ck_restrictions, options);
    auto bounds = boost::copy_range<query::clustering_row_ranges>(wrapping_bounds | boost::adaptors::filtered([&](auto&& r) {
                auto bounds = bound_view::from_range(r);
                return !bound_view::compare(*_schema)(bounds.second, bounds.first);
              })
            | boost::adaptors::transformed([&](auto&& r) { return query::clustering_range(std::move(r));
    }));
    auto less_cmp = clustering_key_prefix::less_compare(*_schema);
    std::sort(bounds.begin(), bounds.end(), [&] (query::clustering_range& x, query::clustering_range& y) {
        if (!x.start() && !y.start()) {
            return false;
        }
        if (!x.start()) {
            return true;
        }
        if (!y.start()) {
            return false;
        }
        return less_cmp(x.start()->value(), y.start()->value());
    });
    auto eq_cmp = clustering_key_prefix::equality(*_schema);
    bounds.erase(std::unique(bounds.begin(), bounds.end(), [&] (query::clustering_range& x, query::clustering_range& y) {
        if (!x.start() && !y.start()) {
            return true;
        }
        if (!x.start() || !y.start()) {
            return false;
        }
        return eq_cmp(x.start()->value(), y.start()->value());
    }), bounds.end());
    return bounds;

}

}
