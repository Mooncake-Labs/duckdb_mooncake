#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "storage/mooncake_table.hpp"
#include "storage/mooncake_table_metadata.hpp"

namespace duckdb {

MooncakeTable::MooncakeTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, uint64_t lsn,
                             Moonlink &moonlink)
    : TableCatalogEntry(catalog, schema, info), lsn(lsn), moonlink(moonlink) {
}

MooncakeTable::~MooncakeTable() = default;

unique_ptr<BaseStatistics> MooncakeTable::GetStatistics(ClientContext &context, column_t column_id) {
	throw NotImplementedException("GetStatistics not implemented");
}

TableStorageInfo MooncakeTable::GetStorageInfo(ClientContext &context) {
	throw NotImplementedException("GetStorageInfo not implemented");
}

MooncakeTableMetadata &MooncakeTable::GetTableMetadata() {
	lock_guard<mutex> guard(lock);
	if (!metadata) {
		metadata = make_uniq<MooncakeTableMetadata>(moonlink, schema.name, name, lsn);
	}
	return *metadata;
}

} // namespace duckdb
