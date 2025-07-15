#include "moonlink/moonlink.hpp"

namespace duckdb {

Moonlink::Moonlink(const string &uri) {
	stream = StreamPtr(moonlink_connect(uri.c_str()).Unwrap());
}

DataPtr Moonlink::GetTableSchema(uint32_t database_id, uint32_t table_id) {
	lock_guard<mutex> guard(lock);
	return DataPtr(moonlink_get_table_schema(stream.get(), database_id, table_id).Unwrap());
}

DataPtr Moonlink::ScanTableBegin(uint32_t database_id, uint32_t table_id) {
	lock_guard<mutex> guard(lock);
	return DataPtr(moonlink_scan_table_begin(stream.get(), database_id, table_id).Unwrap());
}

void Moonlink::ScanTableEnd(uint32_t database_id, uint32_t table_id) {
	lock_guard<mutex> guard(lock);
	moonlink_scan_table_end(stream.get(), database_id, table_id).Unwrap(false /*throw_err*/);
}

} // namespace duckdb
