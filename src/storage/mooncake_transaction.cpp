#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "storage/mooncake_schema.hpp"
#include "storage/mooncake_transaction.hpp"

namespace duckdb {

MooncakeTransaction::MooncakeTransaction(Catalog &catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context) {
	CreateSchemaInfo info;
	info.schema = "main";
	schema = make_uniq<MooncakeSchema>(catalog, info);
}

MooncakeTransaction::~MooncakeTransaction() = default;

SchemaCatalogEntry &MooncakeTransaction::GetSchema() {
	return *schema;
}

} // namespace duckdb
