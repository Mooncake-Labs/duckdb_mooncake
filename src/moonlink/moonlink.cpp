#include "moonlink/moonlink.hpp"

namespace duckdb {

Moonlink::Moonlink(const string &uri) {
	stream = StreamPtr(moonlink_connect(uri.c_str()).Unwrap());
}

DataPtr Moonlink::GetTableSchema(const string &schema, const string &table) {
	lock_guard<mutex> guard(lock);
	return DataPtr(moonlink_get_table_schema(stream.get(), schema.c_str(), table.c_str()).Unwrap());
}

DataPtr Moonlink::ScanTableBegin(const string &schema, const string &table) {
	lock_guard<mutex> guard(lock);
	return DataPtr(moonlink_scan_table_begin(stream.get(), schema.c_str(), table.c_str()).Unwrap());
}

void Moonlink::ScanTableEnd(const string &schema, const string &table) {
	lock_guard<mutex> guard(lock);
	moonlink_scan_table_end(stream.get(), schema.c_str(), table.c_str()).Unwrap(false /*throw_err*/);
}

} // namespace duckdb
