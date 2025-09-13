#include "duckdb/main/extension_helper.hpp"
#include "mooncake_extension.hpp"
#include "pgmooncake.hpp"
#include "storage/mooncake_storage.hpp"

namespace duckdb {

void MooncakeExtension::Load(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	ExtensionHelper::AutoLoadExtension(db, "parquet");
	auto &config = DBConfig::GetConfig(db);
	config.storage_extensions["mooncake"] = make_uniq<MooncakeStorageExtension>();

	string init_query = Pgmooncake::GetInitQuery();
	if (!init_query.empty()) {
		Connection connection(db);
		auto res = connection.Query(init_query);
		if (res->HasError()) {
			res->ThrowError();
		}
	}
}

string MooncakeExtension::Name() {
	return "mooncake";
}

string MooncakeExtension::Version() const {
	return EXT_VERSION_MOONCAKE;
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(mooncake, loader) {
	duckdb::MooncakeExtension().Load(loader);
}
}
