#pragma once

#include "moonlink/moonlink.hpp"

namespace duckdb {

class DeleteFilter;

class MooncakeTableMetadata {
public:
	MooncakeTableMetadata(Moonlink &moonlink, const string &schema, const string &table);

	~MooncakeTableMetadata();

	uint32_t GetNumDataFiles() {
		return data_files_len;
	}

	string GetDataFile(uint32_t data_file_number) {
		return {data_files_data + data_files_offsets[data_file_number],
		        data_files_offsets[data_file_number + 1] - data_files_offsets[data_file_number]};
	}

	unique_ptr<DeleteFilter> GetDeleteFilter(ClientContext &context, uint32_t data_file_number);

private:
	// [data_file_number, puffin_file_number, offset, size]
	using DeletionVector = uint32_t[4];
	// [data_file_number, data_file_row_number]
	using PositionDelete = uint32_t[2];

	Moonlink &moonlink;
	const string &schema;
	const string &table;
	DataPtr data;

	uint32_t data_files_len;
	const uint32_t *data_files_offsets;
	const char *data_files_data;
	uint32_t puffin_files_len;
	const uint32_t *puffin_files_offsets;
	const char *puffin_files_data;
	uint32_t deletion_vectors_len;
	DeletionVector *deletion_vectors;
	uint32_t position_deletes_len;
	PositionDelete *position_deletes;
};

}; // namespace duckdb
