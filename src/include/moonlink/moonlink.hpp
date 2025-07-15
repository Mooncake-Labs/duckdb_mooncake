#pragma once

#include "duckdb/common/mutex.hpp"
#include "moonlink/moonlink_ffi.hpp"

namespace duckdb {

class Moonlink {
public:
	Moonlink(const string &uri);

	DataPtr GetTableSchema(uint32_t database_id, uint32_t table_id);

	DataPtr ScanTableBegin(uint32_t database_id, uint32_t table_id);

	void ScanTableEnd(uint32_t database_id, uint32_t table_id);

private:
	mutex lock;
	StreamPtr stream;
};

} // namespace duckdb
