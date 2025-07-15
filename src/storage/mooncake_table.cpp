#include "duckdb/storage/table_storage_info.hpp"
#include "storage/mooncake_table.hpp"
#include "storage/mooncake_table_metadata.hpp"

namespace duckdb {

MooncakeTable::MooncakeTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Moonlink &moonlink,
                             uint32_t database_id, uint32_t table_id)
    : TableCatalogEntry(catalog, schema, info), moonlink(moonlink), database_id(database_id), table_id(table_id) {
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
		metadata = make_uniq<MooncakeTableMetadata>(moonlink, database_id, table_id);
	}
	return *metadata;
}

} // namespace duckdb
