#include "storage/mooncake_catalog.hpp"
#include "storage/mooncake_storage.hpp"
#include "storage/mooncake_transaction_manager.hpp"

namespace duckdb {

unique_ptr<Catalog> MooncakeAttach(StorageExtensionInfo *storage_info, ClientContext &context, AttachedDatabase &db,
                                   const string &name, AttachInfo &info, AccessMode access_mode) {
	string uri;
	for (auto &entry : info.options) {
		auto key = StringUtil::Lower(entry.first);
		if (key == "type" || key == "read_only") {
			continue;
		} else if (key == "uri") {
			uri = entry.second.ToString();
		} else {
			throw NotImplementedException("Unsupported option %s", entry.first);
		}
	}
	return make_uniq<MooncakeCatalog>(db, std::move(uri));
}

unique_ptr<TransactionManager> MooncakeCreateTransactionManager(StorageExtensionInfo *storage_info,
                                                                AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<MooncakeTransactionManager>(db, catalog);
}

MooncakeStorageExtension::MooncakeStorageExtension() {
	attach = MooncakeAttach;
	create_transaction_manager = MooncakeCreateTransactionManager;
}

} // namespace duckdb
