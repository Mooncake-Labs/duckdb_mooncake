#pragma once

#include "duckdb/common/mutex.hpp"
#include "moonlink/moonlink_ffi.hpp"

namespace duckdb {

class Moonlink {
public:
	Moonlink(const string &uri);

	DataPtr GetTableSchema(const string &schema, const string &table);

	DataPtr ScanTableBegin(const string &schema, const string &table);

	void ScanTableEnd(const string &schema, const string &table);

private:
	mutex lock;
	StreamPtr stream;
};

} // namespace duckdb
