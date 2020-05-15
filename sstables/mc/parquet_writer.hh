#pragma once

#include <parquet4seastar/file_writer.hh>
#include <parquet4seastar/y_combinator.hh>
#include <parquet4seastar/overloaded.hh>
#include <schema.hh>
#include <keys.hh>
#include <mutation_fragment.hh>
#include "sstables/mc/writer.hh"

extern logging::logger parquet_logger;

namespace parquet_writer {

using scylla_schema = ::schema;
using namespace parquet4seastar::logical_type;

parquet4seastar::bytes_view cast_bytes_view(::bytes_view bv) {
    return parquet4seastar::bytes_view(
            reinterpret_cast<const parquet4seastar::byte*>(bv.data()),
            bv.size());
}

parquet4seastar::bytes_view sstring_to_bytes_view(::sstring s) {
    return parquet4seastar::bytes_view(
            reinterpret_cast<const parquet4seastar::byte*>(s.data()),
            s.size());
}

constexpr parquet4seastar::logical_type::logical_type
map_type(abstract_type::kind scylla_type) {
    using kind = abstract_type::kind;
    switch (scylla_type) {
        case kind::ascii:
            return STRING{};
        case kind::boolean:
            return BOOLEAN{};
        case kind::byte:
            return UINT8{};
        case kind::bytes:
            return BYTE_ARRAY{};
        case kind::counter:
            return UNKNOWN{};
        case kind::date:
            return BYTE_ARRAY{};
        case kind::decimal:
            return BYTE_ARRAY{};
        case kind::double_kind:
            return DOUBLE{};
        case kind::duration:
            return BYTE_ARRAY{};
        case kind::empty:
            return UNKNOWN{};
        case kind::float_kind:
            return FLOAT{};
        case kind::inet:
            return BYTE_ARRAY{};
        case kind::int32:
            return INT32{};
        case kind::list:
            return BYTE_ARRAY{};
        case kind::long_kind:
            return INT64{};
        case kind::map:
            return BYTE_ARRAY{};
        case kind::reversed:
            return BYTE_ARRAY{};
        case kind::set:
            return BYTE_ARRAY{};
        case kind::short_kind:
            return INT16{};
        case kind::simple_date:
            return DATE{};
        case kind::time:
            return TIME_INT64{false, TIME_INT64::NANOS};
        case kind::timestamp:
            return TIMESTAMP{false, TIMESTAMP::MILLIS};
        case kind::timeuuid:
            return BYTE_ARRAY{};
        case kind::tuple:
            return BYTE_ARRAY{};
        case kind::user:
            return BYTE_ARRAY{};
        case kind::utf8:
            return STRING{};
        case kind::uuid:
            return BYTE_ARRAY{};
        case kind::varint:
            return BYTE_ARRAY{};
        default:
            return UNKNOWN{};
    }
}

constexpr parquet4seastar::format::Type::type
map_physical_type(abstract_type::kind scylla_type) {
    parquet4seastar::logical_type::logical_type lt =
            map_type(scylla_type);
    parquet4seastar::format::Type::type pt =
            std::visit([] (const auto& x) { return x.physical_type; }, lt);
    return pt;
}

namespace parts {
enum metadata_parts {
    HEADER,
    HEADER_PARTITION_KEY,
    HEADER_PARTITION_KEY_X,
    HEADER_DELETION_LDT,
    HEADER_DELETION_MFDA,
    SROW,
    SROW_FLAGS,
    SROW_EXTENDED_FLAGS,
    SROW_CELLS,
    SROW_CELLS_X,
    SROW_CELLS_X_FLAGS,
    SROW_CELLS_X_DT,
    SROW_CELLS_X_DLDT,
    SROW_CELLS_X_DTTL,
    SROW_CELLS_X_VALUE,
    ROW,
    ROW_FLAGS,
    ROW_EXTENDED_FLAGS,
    ROW_LIVENESS,
    ROW_LIVENESS_DT,
    ROW_LIVENESS_DTTL,
    ROW_LIVENESS_DLDT,
    ROW_DELETION,
    ROW_DELETION_DMFDA,
    ROW_DELETION_DLDT,
    ROW_SHADOWABLE,
    ROW_SHADOWABLE_DMFDA,
    ROW_SHADOWABLE_DLDT,
    ROW_CELLS,
    ROW_CELLS_KEY,
    ROW_CELLS_KEY_X,
    ROW_CELLS_REGULAR,
    ROW_CELLS_REGULAR_X,
    ROW_CELLS_REGULAR_X_FLAGS,
    ROW_CELLS_REGULAR_X_DT,
    ROW_CELLS_REGULAR_X_DLDT,
    ROW_CELLS_REGULAR_X_DTTL,
    ROW_CELLS_REGULAR_X_VALUE,
    ENUM_SIZE,
};
} // namespace parts

struct schema_mapping {
    int def;
    int rep;
    parquet4seastar::logical_type::logical_type pq_type;
};

constexpr schema_mapping schema_mappings[parts::ENUM_SIZE] = {
    {0, 0, UNKNOWN{}}, // HEADER,
    {0, 0, UNKNOWN{}}, // HEADER_PARTITION_KEY,
    {0, 0, UNKNOWN{}}, // HEADER_PARTITION_KEY_X,
    {0, 0, INT32{}}, // HEADER_DELETION_LDT,
    {0, 0, INT64{}}, // HEADER_DELETION_MFDA,
    {1, 0, UNKNOWN{}}, // SROW,
    {1, 0, UINT8{}}, // SROW_FLAGS,
    {2, 0, UINT8{}}, // SROW_EXTENDED_FLAGS,
    {1, 0, UNKNOWN{}}, // SROW_CELLS,
    {2, 0, UNKNOWN{}}, // SROW_CELLS_X,
    {2, 0, UNKNOWN{}}, // SROW_CELLS_X_FLAGS,
    {3, 0, INT64{}}, // SROW_CELLS_X_DT,
    {3, 0, INT32{}}, // SROW_CELLS_X_DLDT,
    {3, 0, INT32{}}, // SROW_CELLS_X_DTTL,
    {3, 0, UNKNOWN{}}, // SROW_CELLS_X_VALUE,
    {1, 1, UNKNOWN{}}, // ROW,
    {1, 1, UINT8{}}, // ROW_FLAGS,
    {2, 1, UINT8{}}, // ROW_EXTENDED_FLAGS,
    {2, 1, UNKNOWN{}}, // ROW_LIVENESS,
    {2, 1, INT64{}}, // ROW_LIVENESS_DT,
    {3, 1, INT32{}}, // ROW_LIVENESS_DTTL,
    {3, 1, INT32{}}, // ROW_LIVENESS_DLDT,
    {2, 1, UNKNOWN{}}, // ROW_DELETION,
    {2, 1, INT64{}}, // ROW_DELETION_DMFDA,
    {2, 1, INT32{}}, // ROW_DELETION_DLDT,
    {2, 1, UNKNOWN{}}, // ROW_SHADOWABLE,
    {2, 1, INT64{}}, // ROW_SHADOWABLE_DMFDA,
    {2, 1, INT32{}}, // ROW_SHADOWABLE_DLDT,
    {1, 1, UNKNOWN{}}, // ROW_CELLS,
    {1, 1, UNKNOWN{}}, // ROW_CELLS_KEY,
    {2, 1, UNKNOWN{}}, // ROW_CELLS_KEY_X,
    {1, 1, UNKNOWN{}}, // ROW_CELLS_REGULAR,
    {2, 1, UNKNOWN{}}, // ROW_CELLS_REGULAR_X,
    {2, 1, UINT8{}}, // ROW_CELLS_REGULAR_X_FLAGS,
    {3, 1, INT64{}}, // ROW_CELLS_REGULAR_X_DT,
    {3, 1, INT32{}}, // ROW_CELLS_REGULAR_X_DLDT,
    {3, 1, INT32{}}, // ROW_CELLS_REGULAR_X_DTTL,
    {3, 1, UNKNOWN{}}, // ROW_CELLS_REGULAR_X_VALUE,
};

struct cell_mapping {
    int value;
    parquet4seastar::logical_type::logical_type pq_type;
    // Fields below are unused for keys
    int flags;
    int dt;
    int dldt;
    int dttl;
};

struct parquet_writer_schema {
    int metadata_mappings[parts::ENUM_SIZE];
    std::vector<cell_mapping> cell_mappings; // Indexed by ordinal id
    parquet4seastar::writer_schema::schema p4s_schema;
    const scylla_schema* scylla_sch;
    int leaves;
};

parquet_writer_schema
scylla_schema_to_parquet_writer_schema(const scylla_schema& scylla_sch) {
    using namespace parquet4seastar;
    using namespace parquet4seastar::writer_schema;
    using namespace parquet4seastar::logical_type;
    using namespace parts;

    auto make_leaf = [] (std::string name, bool optional, logical_type::logical_type lt) {
        primitive_node leaf{};
        leaf.name = std::move(name);
        leaf.optional = optional;
        leaf.logical_type = lt;
        return leaf;
    };

    auto make_struct = [] (std::string name, bool optional) {
        struct_node group{};
        group.name = std::move(name);
        group.optional = optional;
        return group;
    };

    auto make_list = [] (std::string name, bool optional) {
        list_node list{};
        list.name = name;
        list.optional = optional;
        return list;
    };

    parquet_writer_schema pws;
    pws.cell_mappings.resize(scylla_sch.all_columns().size());
    pws.scylla_sch = &scylla_sch;

    int leaf_idx = 0;

    // Types
    for (const auto& col_def : scylla_sch.all_columns()) {
        int id = (int)col_def.ordinal_id;
        logical_type::logical_type pq_type = map_type(col_def.type->get_kind());
        pws.cell_mappings[id].pq_type = pq_type;
    }
    // header
    {
        auto header = make_struct("header", false);
        // header_partition_key
        {
            auto header_partition_key = make_struct("partition_key", false);
            for (const auto& col_def : scylla_sch.partition_key_columns()) {
                int id = (int)col_def.ordinal_id;
                logical_type::logical_type pq_type = pws.cell_mappings[id].pq_type;
                if (std::holds_alternative<logical_type::UNKNOWN>(pq_type)) {
                    continue; // TODO: support all abstract types
                }

                auto header_partition_key_X = make_leaf(
                        col_def.name_as_text(), false, pq_type);
                pws.cell_mappings[id].value = leaf_idx++;
                header_partition_key.fields.push_back(std::move(header_partition_key_X));
            }
            header.fields.push_back(std::move(header_partition_key));
        }
        // header_deletion
        {
            auto header_deletion = make_struct("deletion_time", false);
            header_deletion.fields.push_back(make_leaf(
                    "local_deletion_time", false, schema_mappings[HEADER_DELETION_LDT].pq_type));
            pws.metadata_mappings[HEADER_DELETION_LDT] = leaf_idx++;
            header_deletion.fields.push_back(make_leaf(
                    "marked_for_delete_at", false, schema_mappings[HEADER_DELETION_MFDA].pq_type));
            pws.metadata_mappings[HEADER_DELETION_MFDA] = leaf_idx++;
            header.fields.push_back(std::move(header_deletion));
        }
        pws.p4s_schema.fields.push_back(std::move(header));
    }
    // srow
    {
        auto srow = make_struct("static_row", true);
        srow.fields.push_back(make_leaf(
                "flags", false, schema_mappings[SROW_FLAGS].pq_type));
        pws.metadata_mappings[SROW_FLAGS] = leaf_idx++;
        srow.fields.push_back(make_leaf(
                "extended_flags", true, schema_mappings[SROW_EXTENDED_FLAGS].pq_type));
        pws.metadata_mappings[SROW_EXTENDED_FLAGS] = leaf_idx++;
        auto srow_cells = make_struct("cells", false);
        for (const auto& col_def : scylla_sch.static_columns()) {
            int id = (int)col_def.ordinal_id;
            logical_type::logical_type pq_type = pws.cell_mappings[id].pq_type;
            if (std::holds_alternative<logical_type::UNKNOWN>(pq_type)) {
                continue; // TODO: support all abstract types
            }

            auto srow_cells_X = make_struct(col_def.name_as_text(), true);
            srow_cells_X.fields.push_back(make_leaf(
                    "flags", false, schema_mappings[SROW_CELLS_X_FLAGS].pq_type));
            pws.cell_mappings[id].flags = leaf_idx++;
            srow_cells_X.fields.push_back(make_leaf(
                    "delta_timestamp", true, schema_mappings[SROW_CELLS_X_DT].pq_type));
            pws.cell_mappings[id].dt = leaf_idx++;
            srow_cells_X.fields.push_back(make_leaf(
                    "delta_local_deletion_time", true, schema_mappings[SROW_CELLS_X_DLDT].pq_type));
            pws.cell_mappings[id].dldt = leaf_idx++;
            srow_cells_X.fields.push_back(make_leaf(
                    "delta_ttl", true, schema_mappings[SROW_CELLS_X_DTTL].pq_type));
            pws.cell_mappings[id].dttl = leaf_idx++;
            srow_cells_X.fields.push_back(make_leaf(
                    "value", true, pq_type));
            pws.cell_mappings[id].value = leaf_idx++;
            srow_cells.fields.push_back(std::move(srow_cells_X));
        }
        srow.fields.push_back(std::move(srow_cells));
        pws.p4s_schema.fields.push_back(std::move(srow));
    }
    // row
    {
        auto rows = make_list("rows", false);
        auto row = make_struct("row", false);
        row.fields.push_back(make_leaf(
                "flags", false, schema_mappings[ROW_FLAGS].pq_type));
        pws.metadata_mappings[ROW_FLAGS] = leaf_idx++;
        row.fields.push_back(make_leaf(
                "extended_flags", true, schema_mappings[ROW_EXTENDED_FLAGS].pq_type));
        pws.metadata_mappings[ROW_EXTENDED_FLAGS] = leaf_idx++;

        // row_liveness
        {
            auto row_liveness = make_struct("liveness_info", true);
            row_liveness.fields.push_back(make_leaf(
                    "delta_timestamp", false, schema_mappings[ROW_LIVENESS_DT].pq_type));
            pws.metadata_mappings[ROW_LIVENESS_DT] = leaf_idx++;
            row_liveness.fields.push_back(make_leaf(
                    "delta_ttl", true, schema_mappings[ROW_LIVENESS_DTTL].pq_type));
            pws.metadata_mappings[ROW_LIVENESS_DTTL] = leaf_idx++;
            row_liveness.fields.push_back(make_leaf(
                    "delta_local_deletion_time", true, schema_mappings[ROW_LIVENESS_DLDT].pq_type));
            pws.metadata_mappings[ROW_LIVENESS_DLDT] = leaf_idx++;
            row.fields.push_back(std::move(row_liveness));
        }
        // row_deletion
        {
            auto row_deletion = make_struct("deletion_time", true);
            row_deletion.fields.push_back(make_leaf(
                    "delta_marked_for_delete_at", false, schema_mappings[ROW_DELETION_DMFDA].pq_type));
            pws.metadata_mappings[ROW_DELETION_DMFDA] = leaf_idx++;
            row_deletion.fields.push_back(make_leaf(
                    "delta_local_deletion_time", false, schema_mappings[ROW_DELETION_DLDT].pq_type));
            pws.metadata_mappings[ROW_DELETION_DLDT] = leaf_idx++;
            row.fields.push_back(std::move(row_deletion));
        }
        // row_shadowable
        // TODO: Is storing the shadowable tombstone in separate columns
        // the appropriate metadata_mapping?
        {
            auto row_shadowable = make_struct("shadowable_deletion_time", true);
            row_shadowable.fields.push_back(make_leaf(
                    "delta_marked_for_delete_at", false, schema_mappings[ROW_SHADOWABLE_DMFDA].pq_type));
            pws.metadata_mappings[ROW_SHADOWABLE_DMFDA] = leaf_idx++;
            row_shadowable.fields.push_back(make_leaf(
                    "delta_local_deletion_time", false, schema_mappings[ROW_SHADOWABLE_DLDT].pq_type));
            pws.metadata_mappings[ROW_SHADOWABLE_DLDT] = leaf_idx++;
            row.fields.push_back(std::move(row_shadowable));
        }
        // row_cells_key
        {
            auto row_cells_key = make_struct("clustering_key", false);
            for (const auto& col_def : scylla_sch.clustering_key_columns()) {
                int id = (int)col_def.ordinal_id;
                logical_type::logical_type pq_type = pws.cell_mappings[id].pq_type;
                if (std::holds_alternative<logical_type::UNKNOWN>(pq_type)) {
                    continue; // TODO: support all abstract types
                }

                auto row_cells_key_X = make_leaf(
                        col_def.name_as_text(), true, pq_type);
                pws.cell_mappings[id].value = leaf_idx++;
                row_cells_key.fields.push_back(std::move(row_cells_key_X));
            }
            row.fields.push_back(std::move(row_cells_key));
        }
        // row_cells_regular
        {
            auto row_cells_regular = make_struct("regular", false);
            for (const auto& col_def : scylla_sch.regular_columns()) {
                int id = (int)col_def.ordinal_id;
                logical_type::logical_type pq_type = pws.cell_mappings[id].pq_type;
                if (std::holds_alternative<logical_type::UNKNOWN>(pq_type)) {
                    continue; // TODO: support all abstract types
                }

                auto row_cells_regular_X = make_struct(col_def.name_as_text(), true);
                row_cells_regular_X.fields.push_back(make_leaf(
                        "flags", false, schema_mappings[ROW_CELLS_REGULAR_X_FLAGS].pq_type));
                pws.cell_mappings[id].flags = leaf_idx++;
                row_cells_regular_X.fields.push_back(make_leaf(
                        "delta_timestamp", true, schema_mappings[ROW_CELLS_REGULAR_X_DT].pq_type));
                pws.cell_mappings[id].dt = leaf_idx++;
                row_cells_regular_X.fields.push_back(make_leaf(
                        "delta_local_deletion_time", true, schema_mappings[ROW_CELLS_REGULAR_X_DLDT].pq_type));
                pws.cell_mappings[id].dldt = leaf_idx++;
                row_cells_regular_X.fields.push_back(make_leaf(
                        "delta_ttl", true, schema_mappings[ROW_CELLS_REGULAR_X_DTTL].pq_type));
                pws.cell_mappings[id].dttl = leaf_idx++;
                row_cells_regular_X.fields.push_back(make_leaf(
                        "value", true, pq_type));
                pws.cell_mappings[id].value = leaf_idx++;
                row_cells_regular.fields.push_back(std::move(row_cells_regular_X));
            }
            row.fields.push_back(std::move(row_cells_regular));
        }
        rows.element.reset(new node(std::move(row)));
        pws.p4s_schema.fields.push_back(std::move(rows));
    }

    pws.leaves = leaf_idx;
    return pws;
}

class parquet_writer {
    parquet_writer_schema _pws;
    std::unique_ptr<parquet4seastar::file_writer> _writer;
    std::vector<bool> _cells_written;
    bool _written_srow = false;
    bool _written_row = false;
private:
    parquet_writer() {};
    bool is_current_row_static() {
        return _pws.scylla_sch->static_columns().size() > 0 && !_written_srow;
    }
    int rep() {
        if (is_current_row_static()) {
            return 0;
        } else {
            return _written_row ? 1 : 0;
        }
    }
public:
    ~parquet_writer() {
        close();
    }
    static std::unique_ptr<parquet_writer> open(const scylla_schema& schema) {
        std::unique_ptr<parquet_writer> ret(new parquet_writer());

        std::string filename_base = schema.cf_name().c_str();
        std::replace(filename_base.begin(), filename_base.end(), '/', '-');
        std::string parquet_file = "/tmp/scylla-parquet/" + filename_base + ".parquet.test";
        ret->_pws = scylla_schema_to_parquet_writer_schema(schema);
        ret->_writer = parquet4seastar::file_writer::open(
                parquet_file, ret->_pws.p4s_schema).get0();
        ret->_cells_written.resize(schema.all_columns().size());
        return ret;
    }
    void close() {
        _writer->close().get();
    }
    template <parts::metadata_parts Part, typename ValueType>
    void write_metadata(int def, int rep, ValueType v) {
        constexpr parquet4seastar::logical_type::logical_type lt =
                schema_mappings[Part].pq_type;
        constexpr parquet4seastar::format::Type::type pt =
                std::visit([] (const auto& x) { return x.physical_type; }, lt);
        auto& w = _writer->column<pt>(_pws.metadata_mappings[Part]);
        w.put(def, rep, v);
    }
    template <parts::metadata_parts Part, typename ValueType>
    void write_cell_metadata(int ordinal_id, int def, int rep, ValueType v) {
        const cell_mapping& c = _pws.cell_mappings[ordinal_id];
        int writer_id;
        switch (Part) {
            case parts::SROW_CELLS_X_FLAGS:
            case parts::ROW_CELLS_REGULAR_X_FLAGS:
                writer_id = c.flags;
                break;
            case parts::SROW_CELLS_X_DT:
            case parts::ROW_CELLS_REGULAR_X_DT:
                writer_id = c.dt;
                break;
            case parts::SROW_CELLS_X_DLDT:
            case parts::ROW_CELLS_REGULAR_X_DLDT:
                writer_id = c.dldt;
                break;
            case parts::SROW_CELLS_X_DTTL:
            case parts::ROW_CELLS_REGULAR_X_DTTL:
                writer_id = c.dttl;
                break;
            default:
                throw std::runtime_error("BUG: Not a cell metadata.");
        }
        constexpr parquet4seastar::logical_type::logical_type lt =
                schema_mappings[Part].pq_type;
        constexpr parquet4seastar::format::Type::type pt =
                std::visit([] (const auto& x) { return x.physical_type; }, lt);
        auto& w = _writer->column<pt>(writer_id);
        w.put(def, rep, v);
    }
    void write_header_ldt(int32_t x) {
        write_metadata<parts::HEADER_DELETION_LDT>(0, 0, x);
    }
    void write_header_mfda(int64_t x) {
        write_metadata<parts::HEADER_DELETION_MFDA>(0, 0, x);
    }
    void write_key(const dht::decorated_key& dk) {
        const auto& key = dk.key();
        auto it = key.begin(*_pws.scylla_sch);
        for (const auto& col_def : _pws.scylla_sch->partition_key_columns()) {
            if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[(int)col_def.ordinal_id].pq_type)) {
                ++it;
                continue;
            }
            write_cell_value((int)col_def.ordinal_id, *it);
            ++it;
        }
    }
    void write_row_flags(uint8_t flags) {
        if (is_current_row_static()) {
            write_metadata<parts::SROW_FLAGS>(1, rep(), flags);
        } else {
            write_metadata<parts::ROW_FLAGS>(1, rep(), flags);
        }
    }
    void write_row_extended_flags(uint8_t eflags) {
        if (is_current_row_static()) {
            write_metadata<parts::SROW_EXTENDED_FLAGS>(1, rep(), eflags);
        } else {
            write_metadata<parts::ROW_EXTENDED_FLAGS>(2, rep(), eflags);
        }
    }
    void write_row_extended_flags_empty() {
        if (is_current_row_static()) {
            write_metadata<parts::SROW_EXTENDED_FLAGS>(0, rep(), 0);
        } else {
            write_metadata<parts::ROW_EXTENDED_FLAGS>(1, rep(), 0);
        }
    }
    void write_row_liveness_dt(int64_t dt) {
        write_metadata<parts::ROW_LIVENESS_DT>(2, rep(), dt);
    }
    void write_row_liveness_dttl(int32_t dttl) {
        write_metadata<parts::ROW_LIVENESS_DTTL>(3, rep(), dttl);
    }
    void write_row_liveness_dttl_empty() {
        write_metadata<parts::ROW_LIVENESS_DTTL>(2, rep(), 0);
    }
    void write_row_liveness_dldt(int32_t dldt) {
        write_metadata<parts::ROW_LIVENESS_DLDT>(3, rep(), dldt);
    }
    void write_row_liveness_dldt_empty() {
        write_metadata<parts::ROW_LIVENESS_DLDT>(2, rep(), 0);
    }
    void write_row_liveness_empty() {
        write_metadata<parts::ROW_LIVENESS_DT>(1, rep(), 0);
        write_metadata<parts::ROW_LIVENESS_DLDT>(1, rep(), 0);
        write_metadata<parts::ROW_LIVENESS_DTTL>(1, rep(), 0);
    }
    void write_row_deletion_dmfda(int64_t dmfda) {
        write_metadata<parts::ROW_DELETION_DMFDA>(2, rep(), dmfda);
    }
    void write_row_deletion_dldt(int32_t dldt) {
        write_metadata<parts::ROW_DELETION_DLDT>(2, rep(), dldt);
    }
    void write_row_deletion_empty() {
        write_metadata<parts::ROW_DELETION_DMFDA>(1, rep(), 0);
        write_metadata<parts::ROW_DELETION_DLDT>(1, rep(), 0);
    }
    void write_row_shadowable_dmfda(int64_t dmfda) {
        write_metadata<parts::ROW_SHADOWABLE_DMFDA>(2, rep(), dmfda);
    }
    void write_row_shadowable_dldt(int32_t dldt) {
        write_metadata<parts::ROW_SHADOWABLE_DLDT>(2, rep(), dldt);
    }
    void write_row_shadowable_empty() {
        write_metadata<parts::ROW_SHADOWABLE_DMFDA>(1, rep(), 0);
        write_metadata<parts::ROW_SHADOWABLE_DLDT>(1, rep(), 0);
    }
    void write_srow_empty() {
        write_metadata<parts::SROW_FLAGS>(0, 0, 0);
        write_metadata<parts::SROW_EXTENDED_FLAGS>(0, 0, 0);
        for (const auto& col_def : _pws.scylla_sch->static_columns()) {
            int id = (int)col_def.ordinal_id;
            if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
                continue;
            }
            write_cell_metadata<parts::SROW_CELLS_X_FLAGS>(id, 0, 0, 0);
            write_cell_metadata<parts::SROW_CELLS_X_DT>(id, 0, 0, 0);
            write_cell_metadata<parts::SROW_CELLS_X_DLDT>(id, 0, 0, 0);
            write_cell_metadata<parts::SROW_CELLS_X_DTTL>(id, 0, 0, 0);
        }
    }
    void write_clustering_key(const clustering_key_prefix& key) {
        auto it = key.begin(*_pws.scylla_sch);
        auto end = key.end(*_pws.scylla_sch);
        for (const auto& col_def : _pws.scylla_sch->clustering_key_columns()) {
            int id = (int)col_def.ordinal_id;
            if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
                if (it != end) {
                    ++it;
                }
                continue;
            }
            if (it != end) {
                write_cell_value(id, *it);
                ++it;
            } else {
                write_cell_value_empty(id);
            }
        }
    }
    void write_cell_flags(int id, uint8_t flags) {
        if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
            return;
        }
        write_cell_metadata<parts::ROW_CELLS_REGULAR_X_FLAGS>(id, 2, rep(), flags);
    }
    void write_cell_dt(int id, int64_t dt) {
        if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
            return;
        }
        write_cell_metadata<parts::ROW_CELLS_REGULAR_X_DT>(id, 3, rep(), dt);
    }
    void write_cell_dt_empty(int id) {
        if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
            return;
        }
        write_cell_metadata<parts::ROW_CELLS_REGULAR_X_DT>(id, 2, rep(), 0);
    }
    void write_cell_dldt(int id, int32_t dldt) {
        if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
            return;
        }
        write_cell_metadata<parts::ROW_CELLS_REGULAR_X_DLDT>(id, 3, rep(), dldt);
    }
    void write_cell_dldt_empty(int id) {
        if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
            return;
        }
        write_cell_metadata<parts::ROW_CELLS_REGULAR_X_DLDT>(id, 2, rep(), 0);
    }
    void write_cell_dttl(int id, int32_t dttl) {
        if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
            return;
        }
        write_cell_metadata<parts::ROW_CELLS_REGULAR_X_DTTL>(id, 3, rep(), dttl);
    }
    void write_cell_dttl_empty(int id) {
        if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
            return;
        }
        write_cell_metadata<parts::ROW_CELLS_REGULAR_X_DTTL>(id, 2, rep(), 0);
    }
    void write_cell_value(int ordinal_id, atomic_cell_value_view v) {
        bytes b = v.linearize();
        write_cell_value(ordinal_id, b);
    }
    void write_cell_value(int ordinal_id, bytes_view v) {
        if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[ordinal_id].pq_type)) {
            return;
        }

        using kind = abstract_type::kind;

        const cell_mapping& c = _pws.cell_mappings[ordinal_id];
        (void)c;
        const auto& col_def = _pws.scylla_sch->column_at((ordinal_column_id)ordinal_id);
        int def;
        int rep;
        int writer_id = _pws.cell_mappings[ordinal_id].value;
        if (col_def.is_partition_key()) {
            def = schema_mappings[parts::HEADER_PARTITION_KEY_X].def;
            rep = schema_mappings[parts::HEADER_PARTITION_KEY_X].rep;
        } else if (col_def.is_static()) {
            def = schema_mappings[parts::SROW_CELLS_X_VALUE].def;
            rep = schema_mappings[parts::SROW_CELLS_X_VALUE].rep;
        } else if (col_def.is_clustering_key()) {
            def = schema_mappings[parts::ROW_CELLS_KEY_X].def;
            rep = schema_mappings[parts::ROW_CELLS_KEY_X].rep;
        } else if (col_def.is_regular()) {
            def = schema_mappings[parts::ROW_CELLS_REGULAR_X_VALUE].def;
            rep = schema_mappings[parts::ROW_CELLS_REGULAR_X_VALUE].rep;
        }

        try {
            data_value dv = col_def.type->deserialize_value(v);

            switch (col_def.type->get_kind()) {
            case kind::counter:
            case kind::empty: break;

            case kind::date: {
                auto& w = _writer->column<map_physical_type(kind::date)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::decimal: {
                auto& w = _writer->column<map_physical_type(kind::decimal)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::duration: {
                auto& w = _writer->column<map_physical_type(kind::duration)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::inet: {
                auto& w = _writer->column<map_physical_type(kind::inet)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::list: {
                auto& w = _writer->column<map_physical_type(kind::list)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::map: {
                auto& w = _writer->column<map_physical_type(kind::map)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::reversed: {
                auto& w = _writer->column<map_physical_type(kind::reversed)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::set: {
                auto& w = _writer->column<map_physical_type(kind::set)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::timeuuid: {
                auto& w = _writer->column<map_physical_type(kind::timeuuid)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::tuple: {
                auto& w = _writer->column<map_physical_type(kind::tuple)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::user: {
                auto& w = _writer->column<map_physical_type(kind::user)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::uuid: {
                auto& w = _writer->column<map_physical_type(kind::uuid)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::varint: {
                auto& w = _writer->column<map_physical_type(kind::varint)>(writer_id);
                w.put(def, rep, cast_bytes_view(v));
                break;
            }
            case kind::ascii: {
                auto& w = _writer->column<map_physical_type(kind::ascii)>(writer_id);
                auto typed_value = value_cast<sstring>(dv);
                w.put(def, rep, sstring_to_bytes_view(typed_value));
                break;
            }
            case kind::boolean: {
                auto& w = _writer->column<map_physical_type(kind::boolean)>(writer_id);
                auto typed_value = value_cast<bool>(dv);
                w.put(def, rep, typed_value);
                break;
            }
            case kind::byte: {
                auto& w = _writer->column<map_physical_type(kind::byte)>(writer_id);
                auto typed_value = value_cast<int8_t>(dv);
                w.put(def, rep, typed_value);
                break;
            }
            case kind::bytes: {
                auto& w = _writer->column<map_physical_type(kind::bytes)>(writer_id);
                auto typed_value = value_cast<bytes>(dv);
                w.put(def, rep, cast_bytes_view(typed_value));
                break;
            }
            case kind::double_kind: {
                auto& w = _writer->column<map_physical_type(kind::double_kind)>(writer_id);
                auto typed_value = value_cast<double>(dv);
                w.put(def, rep, typed_value);
                break;
            }
            case kind::float_kind:{
                auto& w = _writer->column<map_physical_type(kind::float_kind)>(writer_id);
                auto typed_value = value_cast<float>(dv);
                w.put(def, rep, typed_value);
                break;
            }
            case kind::int32: {
                auto& w = _writer->column<map_physical_type(kind::int32)>(writer_id);
                auto typed_value = value_cast<int32_t>(dv);
                w.put(def, rep, typed_value);
                break;
            }
            case kind::long_kind: {
                auto& w = _writer->column<map_physical_type(kind::long_kind)>(writer_id);
                auto typed_value = value_cast<int64_t>(dv);
                w.put(def, rep, typed_value);
                break;
            }
            case kind::short_kind: {
                auto& w = _writer->column<map_physical_type(kind::short_kind)>(writer_id);
                auto typed_value = value_cast<int16_t>(dv);
                w.put(def, rep, typed_value);
                break;
            }
            case kind::simple_date: {
                auto& w = _writer->column<map_physical_type(kind::simple_date)>(writer_id);
                auto typed_value = value_cast<uint32_t>(dv);
                w.put(def, rep, typed_value);
                break;
            }
            case kind::time: {
                auto& w = _writer->column<map_physical_type(kind::time)>(writer_id);
                auto typed_value = value_cast<int64_t>(dv);
                w.put(def, rep, typed_value);
                break;
            }
            case kind::timestamp: {
                auto& w = _writer->column<map_physical_type(kind::timestamp)>(writer_id);
                auto typed_value = value_cast<db_clock::time_point>(dv);
                int64_t millis = typed_value.time_since_epoch().count();
                w.put(def, rep, millis);
                break;
            }
            case kind::utf8:{
                auto& w = _writer->column<map_physical_type(kind::utf8)>(writer_id);
                auto typed_value = value_cast<sstring>(dv);
                w.put(def, rep, sstring_to_bytes_view(typed_value));
                break;
            }
            }
        } catch (const std::bad_cast& e) {
            parquet_logger.error("Deserialization error in table {}.{}, column {}, type {},  ordinal_id {}, parquet column {}",
                    _pws.scylla_sch->ks_name(), _pws.scylla_sch->cf_name(), col_def.name_as_text(), col_def.type->name(), ordinal_id, writer_id);
            write_cell_value_empty(ordinal_id);
        }
    }
    void write_cell_value_empty(int id) {
        if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
            return;
        }
        std::visit(parquet4seastar::overloaded {
            [&] (const auto& lt) {
                constexpr parquet4seastar::format::Type::type pt = lt.physical_type;
                auto& w = _writer->column<pt>(_pws.cell_mappings[id].value);
                using input_type = typename std::remove_reference_t<decltype(w)>::input_type;
                w.put(1, rep(), input_type{});
            },
            [&] (const parquet4seastar::logical_type::INT96&) {
                // unreachable
            },
        }, _pws.cell_mappings[id].pq_type);
    }
    void write_cell_empty(int id) {
        write_cell_value_empty(id);
        write_cell_metadata<parts::ROW_CELLS_REGULAR_X_FLAGS>(id, 1, rep(), 0);
        write_cell_metadata<parts::ROW_CELLS_REGULAR_X_DT>(id, 1, rep(), 0);
        write_cell_metadata<parts::ROW_CELLS_REGULAR_X_DLDT>(id, 1, rep(), 0);
        write_cell_metadata<parts::ROW_CELLS_REGULAR_X_DTTL>(id, 1, rep(), 0);
    }
    void write_row_fill() {
        for (const auto& col_def : _pws.scylla_sch->regular_columns()) {
            int id = (int)col_def.ordinal_id;
            if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
                continue;
            }
            if (_cells_written[id]) {
                _cells_written[id] = false;
                continue;
            }
            write_cell_empty(id);
        }
    }
    void write_srow_fill() {
        for (const auto& col_def : _pws.scylla_sch->static_columns()) {
            int id = (int)col_def.ordinal_id;
            if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
                continue;
            }
            if (_cells_written[id]) {
                _cells_written[id] = false;
                continue;
            }
            write_cell_empty(id);
        }
    }
    void write_rows_empty() {
        write_metadata<parts::ROW_FLAGS>(0, 0, 0);
        write_metadata<parts::ROW_EXTENDED_FLAGS>(0, 0, 0);
        write_metadata<parts::ROW_LIVENESS_DT>(0, 0, 0);
        write_metadata<parts::ROW_LIVENESS_DLDT>(0, 0, 0);
        write_metadata<parts::ROW_LIVENESS_DTTL>(0, 0, 0);
        write_metadata<parts::ROW_DELETION_DMFDA>(0, 0, 0);
        write_metadata<parts::ROW_DELETION_DLDT>(0, 0, 0);
        write_metadata<parts::ROW_SHADOWABLE_DMFDA>(0, 0, 0);
        write_metadata<parts::ROW_SHADOWABLE_DLDT>(0, 0, 0);
        for (const auto& col_def : _pws.scylla_sch->regular_columns()) {
            int id = (int)col_def.ordinal_id;
            if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
                continue;
            }
            std::visit(parquet4seastar::overloaded {
                [&] (const auto& lt) {
                    constexpr parquet4seastar::format::Type::type pt = lt.physical_type;
                    auto& w = _writer->column<pt>(_pws.cell_mappings[id].value);
                    using input_type = typename std::remove_reference_t<decltype(w)>::input_type;
                    w.put(0, 0, input_type{});
                },
                [&] (const parquet4seastar::logical_type::INT96&) {
                    // unreachable
                },
            }, _pws.cell_mappings[id].pq_type);
            write_cell_metadata<parts::ROW_CELLS_REGULAR_X_FLAGS>(id, 0, 0, 0);
            write_cell_metadata<parts::ROW_CELLS_REGULAR_X_DT>(id, 0, 0, 0);
            write_cell_metadata<parts::ROW_CELLS_REGULAR_X_DLDT>(id, 0, 0, 0);
            write_cell_metadata<parts::ROW_CELLS_REGULAR_X_DTTL>(id, 0, 0, 0);
        }
        for (const auto& col_def : _pws.scylla_sch->clustering_key_columns()) {
            int id = (int)col_def.ordinal_id;
            if (std::holds_alternative<UNKNOWN>(_pws.cell_mappings[id].pq_type)) {
                continue;
            }
            std::visit(parquet4seastar::overloaded {
                [&] (const auto& lt) {
                    constexpr parquet4seastar::format::Type::type pt = lt.physical_type;
                    auto& w = _writer->column<pt>(_pws.cell_mappings[id].value);
                    using input_type = typename std::remove_reference_t<decltype(w)>::input_type;
                    w.put(0, 0, input_type{});
                },
                [&] (const parquet4seastar::logical_type::INT96&) {
                    // unreachable
                },
            }, _pws.cell_mappings[id].pq_type);
        }
    }
    void write_empty() {
        using namespace parquet4seastar::logical_type;
    }
    void finish_clustering_row() {
        write_row_fill();
        _written_row = true;
    }
    void finish_cell(int ordinal_id) {
        _cells_written[ordinal_id] = true;
    }
    void finish_static_row() {
        write_srow_fill();
        _written_srow = true;
    }
    void finish_partition() {
        if (!_written_row) {
            write_rows_empty();
        }
        if (!_written_srow) {
            write_srow_empty();
        }
        _written_row = false;
        _written_srow = false;
    }
};

} // namespace parquet_writer
