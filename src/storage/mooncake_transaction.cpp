#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "storage/mooncake_schema.hpp"
#include "storage/mooncake_transaction.hpp"

namespace duckdb {

MooncakeTransaction::MooncakeTransaction(Catalog &catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), catalog(catalog) {
}

MooncakeTransaction::~MooncakeTransaction() = default;

SchemaCatalogEntry &MooncakeTransaction::GetOrCreateSchema(const string &name) {
	lock_guard<mutex> guard(lock);
	if (auto it = schemas.find(name); it != schemas.end()) {
		return *it->second.get();
	}
	CreateSchemaInfo info;
	info.schema = name;
	schemas[name] = make_uniq<MooncakeSchema>(catalog, info);
	return *schemas[name];
}

} // namespace duckdb
