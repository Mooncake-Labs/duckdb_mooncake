#include "duckdb/common/re2_regex.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "moonlink/moonlink.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"
#include "storage/mooncake_catalog.hpp"
#include "storage/mooncake_schema.hpp"
#include "storage/mooncake_table.hpp"

namespace duckdb {

MooncakeSchema::MooncakeSchema(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), moonlink(catalog.Cast<MooncakeCatalog>().GetMoonlink()) {
}

MooncakeSchema::~MooncakeSchema() = default;

void MooncakeSchema::Scan(ClientContext &context, CatalogType type,
                          const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan not implemented");
}

void MooncakeSchema::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan not implemented");
}

optional_ptr<CatalogEntry> MooncakeSchema::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                       TableCatalogEntry &table) {
	throw NotImplementedException("CreateIndex not implemented");
}

optional_ptr<CatalogEntry> MooncakeSchema::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw NotImplementedException("CreateFunction not implemented");
}

optional_ptr<CatalogEntry> MooncakeSchema::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw NotImplementedException("CreateTable not implemented");
}

optional_ptr<CatalogEntry> MooncakeSchema::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("CreateView not implemented");
}

optional_ptr<CatalogEntry> MooncakeSchema::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw NotImplementedException("CreateSequence not implemented");
}

optional_ptr<CatalogEntry> MooncakeSchema::CreateTableFunction(CatalogTransaction transaction,
                                                               CreateTableFunctionInfo &info) {
	throw NotImplementedException("CreateTableFunction not implemented");
}

optional_ptr<CatalogEntry> MooncakeSchema::CreateCopyFunction(CatalogTransaction transaction,
                                                              CreateCopyFunctionInfo &info) {
	throw NotImplementedException("CreateCopyFunction not implemented");
}

optional_ptr<CatalogEntry> MooncakeSchema::CreatePragmaFunction(CatalogTransaction transaction,
                                                                CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("CreatePragmaFunction not implemented");
}

optional_ptr<CatalogEntry> MooncakeSchema::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw NotImplementedException("CreateCollation not implemented");
}

optional_ptr<CatalogEntry> MooncakeSchema::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("CreateType not implemented");
}

optional_ptr<CatalogEntry> MooncakeSchema::LookupEntry(CatalogTransaction transaction,
                                                       const EntryLookupInfo &lookup_info) {
	lock_guard<mutex> guard(lock);
	auto entry = tables.find(lookup_info.GetEntryName());
	if (entry != tables.end()) {
		return entry->second.get();
	}

	duckdb_re2::Match match;
	duckdb_re2::Regex regex(R"((\d+)\.(\d+))");
	if (!RegexMatch(lookup_info.GetEntryName(), match, regex)) {
		throw CatalogException(lookup_info.GetErrorContext(), "Table %s not found", lookup_info.GetEntryName());
	}
	unsigned long id = std::stoul(match[1].str());
	if (id > std::numeric_limits<uint32_t>::max()) {
		throw CatalogException(lookup_info.GetErrorContext(), "Table %s not found", lookup_info.GetEntryName());
	}
	uint32_t database_id = static_cast<uint32_t>(id);
	id = std::stoul(match[2].str());
	if (id > std::numeric_limits<uint32_t>::max()) {
		throw CatalogException(lookup_info.GetErrorContext(), "Table %s not found", lookup_info.GetEntryName());
	}
	uint32_t table_id = static_cast<uint32_t>(id);
	DataPtr data = moonlink.GetTableSchema(database_id, table_id);

	nanoarrow::ipc::UniqueDecoder decoder;
	ArrowSchemaWrapper schema;
	ArrowError error;
	if (ArrowIpcDecoderInit(decoder.get()) ||
	    ArrowIpcDecoderDecodeHeader(decoder.get(), {data->ptr, static_cast<int64_t>(data->len)}, &error) ||
	    ArrowIpcDecoderDecodeSchema(decoder.get(), &schema.arrow_schema, &error)) {
		throw InternalException("Error decoding schema: %s", ArrowErrorMessage(&error));
	}

	ArrowTableType arrow_table;
	vector<string> names;
	vector<LogicalType> return_types;
	ArrowTableFunction::PopulateArrowTableType(transaction.db->config, arrow_table, schema, names, return_types);

	CreateTableInfo table_info;
	table_info.table = lookup_info.GetEntryName();
	for (idx_t i = 0; i < names.size(); i++) {
		table_info.columns.AddColumn(ColumnDefinition(names[i], return_types[i]));
	}
	auto table = make_uniq<MooncakeTable>(catalog, *this, table_info, moonlink, database_id, table_id);
	auto &result = *table;
	tables.emplace(lookup_info.GetEntryName(), std::move(table));
	return result;
}

void MooncakeSchema::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("DropEntry not implemented");
}

void MooncakeSchema::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("Alter not implemented");
}

} // namespace duckdb
