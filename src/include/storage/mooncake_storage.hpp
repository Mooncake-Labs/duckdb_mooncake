#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class MooncakeStorageExtension : public StorageExtension {
public:
	MooncakeStorageExtension();
};

} // namespace duckdb
