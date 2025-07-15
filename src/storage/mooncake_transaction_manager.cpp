#include "storage/mooncake_transaction.hpp"
#include "storage/mooncake_transaction_manager.hpp"

namespace duckdb {

MooncakeTransactionManager::MooncakeTransactionManager(AttachedDatabase &db, Catalog &catalog)
    : TransactionManager(db), catalog(catalog) {
}

MooncakeTransactionManager::~MooncakeTransactionManager() = default;

Transaction &MooncakeTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<MooncakeTransaction>(catalog, *this, context);
	auto &result = *transaction;
	lock_guard<mutex> guard(lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData MooncakeTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	lock_guard<mutex> guard(lock);
	transactions.erase(transaction);
	return ErrorData();
}

void MooncakeTransactionManager::RollbackTransaction(Transaction &transaction) {
	lock_guard<mutex> guard(lock);
	transactions.erase(transaction);
}

void MooncakeTransactionManager::Checkpoint(ClientContext &context, bool force) {
	throw NotImplementedException("Checkpoint not implemented");
}

} // namespace duckdb
