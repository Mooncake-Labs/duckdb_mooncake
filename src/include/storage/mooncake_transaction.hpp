#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

class MooncakeTransaction : public Transaction {
public:
	MooncakeTransaction(Catalog &catalog, TransactionManager &manager, ClientContext &context);

	~MooncakeTransaction();

	SchemaCatalogEntry &GetSchema();

private:
	unique_ptr<SchemaCatalogEntry> schema;
};

} // namespace duckdb
