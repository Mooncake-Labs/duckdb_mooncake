#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

class MooncakeTransaction : public Transaction {
public:
	MooncakeTransaction(Catalog &catalog, TransactionManager &manager, ClientContext &context);

	~MooncakeTransaction();

	SchemaCatalogEntry &GetOrCreateSchema(const string &name);

private:
	Catalog &catalog;
	mutex lock;
	case_insensitive_map_t<unique_ptr<SchemaCatalogEntry>> schemas;
};

} // namespace duckdb
