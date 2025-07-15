#include "duckdb/storage/database_size.hpp"
#include "moonlink/moonlink.hpp"
#include "storage/mooncake_catalog.hpp"
#include "storage/mooncake_transaction.hpp"

namespace duckdb {

MooncakeCatalog::MooncakeCatalog(AttachedDatabase &db, string _uri)
    : Catalog(db), uri(std::move(_uri)), moonlink(make_uniq<Moonlink>(uri)) {
}

MooncakeCatalog::~MooncakeCatalog() = default;

void MooncakeCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> MooncakeCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("CreateSchema not implemented");
}

optional_ptr<SchemaCatalogEntry> MooncakeCatalog::LookupSchema(CatalogTransaction transaction,
                                                               const EntryLookupInfo &schema_lookup,
                                                               OnEntryNotFound if_not_found) {
	if (schema_lookup.GetEntryName() == "main") {
		return transaction.transaction->Cast<MooncakeTransaction>().GetSchema();
	} else if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw CatalogException(schema_lookup.GetErrorContext(), "Schema %s not found", schema_lookup.GetEntryName());
	} else {
		return nullptr;
	}
}

void MooncakeCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	throw NotImplementedException("ScanSchemas not implemented");
}

PhysicalOperator &MooncakeCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                     LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("PlanCreateTableAs not implemented");
}

PhysicalOperator &MooncakeCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                              optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("PlanInsert not implemented");
}

PhysicalOperator &MooncakeCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                              PhysicalOperator &plan) {
	throw NotImplementedException("PlanDelete not implemented");
}

PhysicalOperator &MooncakeCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                              PhysicalOperator &plan) {
	throw NotImplementedException("PlanUpdate not implemented");
}

unique_ptr<LogicalOperator> MooncakeCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                             TableCatalogEntry &table,
                                                             unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("BindCreateIndex not implemented");
}

DatabaseSize MooncakeCatalog::GetDatabaseSize(ClientContext &context) {
	throw NotImplementedException("GetDatabaseSize not implemented");
}

bool MooncakeCatalog::InMemory() {
	return false;
}

string MooncakeCatalog::GetDBPath() {
	return uri;
}

void MooncakeCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("DropSchema not implemented");
}

} // namespace duckdb
