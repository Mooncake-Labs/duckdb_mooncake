#pragma once

#include "duckdb/common/unique_ptr.hpp"

struct Data {
	uint8_t *ptr;
	size_t len;
	size_t capacity;
};

template <typename T>
struct Result {
	T Unwrap(bool throw_err = true);

	enum class Tag {
		Ok,
		Err,
	};

	struct Ok {
		T t;
	};

	struct Err {
		char *msg;
	};

	Tag tag;
	union {
		Ok ok;
		Err err;
	};
};

struct Stream;

struct Void {
	uint8_t _void;
};

extern "C" [[nodiscard]] Result<Stream *> moonlink_connect(const char *uri);

extern "C" void moonlink_drop_cstring(char *cstring);

extern "C" void moonlink_drop_data(Data *data);

extern "C" void moonlink_drop_stream(Stream *stream);

extern "C" [[nodiscard]] Result<Data *> moonlink_get_table_schema(Stream *stream, const char *database,
                                                                  const char *schema, const char *table);

extern "C" [[nodiscard]] Result<Data *> moonlink_scan_table_begin(Stream *stream, const char *database,
                                                                  const char *schema, const char *table, uint64_t lsn);

extern "C" [[nodiscard]] Result<Void> moonlink_scan_table_end(Stream *stream, const char *database, const char *schema,
                                                              const char *table);

template <auto fn>
struct Deleter {
	template <typename T>
	void operator()(T *t) const {
		fn(t);
	}
};

using DataPtr = duckdb::unique_ptr<Data, Deleter<moonlink_drop_data>>;

using StreamPtr = duckdb::unique_ptr<Stream, Deleter<moonlink_drop_stream>>;

template <typename T>
T Result<T>::Unwrap(bool throw_err) {
	if (tag == Tag::Err) {
		duckdb::string msg = err.msg;
		moonlink_drop_cstring(err.msg);
		if (throw_err) {
			throw duckdb::Exception(duckdb::ExceptionType::UNKNOWN_TYPE, msg);
		}
	}
	return ok.t;
}
