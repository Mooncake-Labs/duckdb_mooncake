#include "moonlink/moonlink.hpp"

namespace duckdb {

Moonlink::Moonlink(string uri, string database) : database(database) {
	stream = StreamPtr(moonlink_connect(uri.c_str()).Unwrap());
}

DataPtr Moonlink::GetTableSchema(const string &schema, const string &table) {
	lock_guard<mutex> guard(lock);
	return DataPtr(moonlink_get_table_schema(stream.get(), database.c_str(), schema.c_str(), table.c_str()).Unwrap());
}

DataPtr Moonlink::ScanTableBegin(const string &schema, const string &table, uint64_t lsn) {
	lock_guard<mutex> guard(lock);
	return DataPtr(
	    moonlink_scan_table_begin(stream.get(), database.c_str(), schema.c_str(), table.c_str(), lsn).Unwrap());
}

void Moonlink::ScanTableEnd(const string &schema, const string &table) {
	lock_guard<mutex> guard(lock);
	moonlink_scan_table_end(stream.get(), database.c_str(), schema.c_str(), table.c_str()).Unwrap(false /*throw_err*/);
}

} // namespace duckdb
