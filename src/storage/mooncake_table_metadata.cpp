#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "roaring/roaring.hh"
#include "storage/mooncake_table_metadata.hpp"

using roaring::BulkContext;
using roaring::Roaring;

namespace duckdb {

MooncakeTableMetadata::MooncakeTableMetadata(Moonlink &moonlink, const string &schema, const string &table,
                                             uint64_t lsn)
    : moonlink(moonlink), schema(schema), table(table) {
	data = moonlink.ScanTableBegin(schema, table, lsn);
	uint32_t *ptr = reinterpret_cast<uint32_t *>(data->ptr);

	data_files_len = *ptr++;
	data_files_offsets = ptr;
	ptr += data_files_len + 1;

	puffin_files_len = *ptr++;
	puffin_files_offsets = ptr;
	ptr += puffin_files_len + 1;

	deletion_vectors_len = *ptr++;
	deletion_vectors = reinterpret_cast<DeletionVector *>(ptr);
	ptr += 4 * deletion_vectors_len;

	position_deletes_len = *ptr++;
	position_deletes = reinterpret_cast<PositionDelete *>(ptr);
	ptr += 2 * position_deletes_len;

	char *char_ptr = reinterpret_cast<char *>(ptr);
	data_files_data = char_ptr;
	char_ptr += data_files_offsets[data_files_len];
	puffin_files_data = char_ptr;
	char_ptr += puffin_files_offsets[puffin_files_len];
	D_ASSERT(char_ptr == reinterpret_cast<char *>(data->ptr + data->len));
}

MooncakeTableMetadata::~MooncakeTableMetadata() {
	moonlink.ScanTableEnd(schema, table);
}

class MooncakeDeleteFilter : public DeleteFilter {
public:
	MooncakeDeleteFilter(Roaring _roaring) : roaring(std::move(_roaring)) {
	}

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override {
		result_sel.Initialize(count);
		idx_t result_count = 0;
		BulkContext bulk_context;
		for (idx_t i = 0; i < count; i++) {
			if (!roaring.containsBulk(bulk_context, start_row_index + i)) {
				result_sel.set_index(result_count++, i);
			}
		}
		return result_count;
	}

private:
	Roaring roaring;
};

unique_ptr<DeleteFilter> MooncakeTableMetadata::GetDeleteFilter(ClientContext &context, uint32_t data_file_number) {
	Roaring roaring;
	auto dvit = std::lower_bound(deletion_vectors, deletion_vectors + deletion_vectors_len, data_file_number,
	                             [](const DeletionVector &dv, uint32_t v) { return dv[0] < v; });
	if (dvit < deletion_vectors + deletion_vectors_len && (*dvit)[0] == data_file_number) {
		auto [_data_file_number, puffin_file_number, offset, size] = *dvit;
		string puffin_file(puffin_files_data + puffin_files_offsets[puffin_file_number],
		                   puffin_files_offsets[puffin_file_number + 1] - puffin_files_offsets[puffin_file_number]);
		auto &fs = FileSystem::GetFileSystem(context);
		auto file_handle = fs.OpenFile(puffin_file, FileOpenFlags(FileOpenFlags::FILE_FLAGS_READ));
		// | 4-byte length | 4-byte magic | 8-byte #keys = 1 | 4-byte key = 0 | buffer | 4-byte CRC-32 |
		std::vector<char> buffer(size - 24);
		file_handle->Read(buffer.data(), size - 24, offset + 20);
		roaring = Roaring::read(buffer.data());
	}
	auto pdit = std::lower_bound(position_deletes, position_deletes + position_deletes_len, data_file_number,
	                             [](const PositionDelete &pd, uint32_t v) { return pd[0] < v; });
	BulkContext bulk_context;
	for (; pdit < position_deletes + position_deletes_len && (*pdit)[0] == data_file_number; pdit++) {
		roaring.addBulk(bulk_context, (*pdit)[1]);
	}
	return roaring.isEmpty() ? nullptr : make_uniq<MooncakeDeleteFilter>(std::move(roaring));
}

} // namespace duckdb
