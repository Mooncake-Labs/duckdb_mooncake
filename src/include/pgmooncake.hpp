#pragma once

#include "duckdb/common/dl.hpp"

namespace duckdb {

class Pgmooncake {
public:
	using drop_cstring_fn = void (*)(char *);
	using get_init_query_fn = char *(*)();
	using get_lsn_fn = uint64_t (*)();

	static string GetInitQuery() {
		static drop_cstring_fn drop_cstring =
		    reinterpret_cast<drop_cstring_fn>(dlsym(RTLD_DEFAULT, "pgmooncake_drop_cstring"));
		static get_init_query_fn get_init_query =
		    reinterpret_cast<get_init_query_fn>(dlsym(RTLD_DEFAULT, "pgmooncake_get_init_query"));

		if (!get_init_query) {
			return "";
		}
		char *init_query = get_init_query();
		string res(init_query);
		D_ASSERT(drop_cstring);
		drop_cstring(init_query);
		return res;
	}

	static uint64_t GetLsn() {
		static get_lsn_fn get_lsn = reinterpret_cast<get_lsn_fn>(dlsym(RTLD_DEFAULT, "pgmooncake_get_lsn"));

		return get_lsn ? get_lsn() : 0;
	}
};

} // namespace duckdb
