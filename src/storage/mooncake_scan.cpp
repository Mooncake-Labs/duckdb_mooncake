#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "parquet_reader.hpp"
#include "storage/mooncake_table.hpp"
#include "storage/mooncake_table_metadata.hpp"

namespace duckdb {

struct DataFileStatistics : public ObjectCacheEntry {
public:
	DataFileStatistics(ClientContext &context, string data_file, const vector<string> &names) : column_stats() {
		ParquetReader reader(context, data_file, ParquetOptions(context));
		for (auto &name : names) {
			column_stats[name] = reader.ReadStatistics(name);
		}
	}

	static string ObjectType() {
		return "data_file_statistics";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	unordered_map<string, unique_ptr<BaseStatistics>> column_stats;
};

static ObjectCache mooncake_stats;

struct MooncakeMultiFileList : public MultiFileList {
	MooncakeMultiFileList(MooncakeTable &_table)
	    : MultiFileList({}, FileGlobOptions::ALLOW_EMPTY), table(_table), metadata(), data_files(),
	      data_file_numbers() {
	}

	// Lazily initialized because pg_duckdb binds each query three times
	void LazyInitialize(ClientContext &context, const vector<string> &names, const vector<column_t> &column_ids,
	                    optional_ptr<TableFilterSet> filters) {
		metadata = &table.GetTableMetadata();
		for (uint32_t data_file_number = 0; data_file_number < metadata->GetNumDataFiles(); data_file_number++) {
			auto data_file = metadata->GetDataFile(data_file_number);
			if (filters) {
				auto file_stats = mooncake_stats.GetOrCreate<DataFileStatistics>(data_file, context, data_file, names);
				auto skip_file = [&](auto &entry) {
					if (IsVirtualColumn(column_ids[entry.first])) {
						return false;
					}
					auto &stats = file_stats->column_stats.at(names[column_ids[entry.first]]);
					return stats && entry.second->CheckStatistics(*stats) == FilterPropagateResult::FILTER_ALWAYS_FALSE;
				};
				if (any_of(filters->filters.begin(), filters->filters.end(), skip_file)) {
					continue;
				}
			}
			data_files.push_back(data_file);
			data_file_numbers.push_back(data_file_number);
		}
	}

	vector<OpenFileInfo> GetAllFiles() override {
		return data_files;
	}

	FileExpandResult GetExpandResult() override {
		return FileExpandResult::MULTIPLE_FILES;
	}

	idx_t GetTotalFileCount() override {
		return data_files.size();
	}

	unique_ptr<MultiFileList> Copy() override {
		D_ASSERT(data_files.empty());
		return make_uniq<MooncakeMultiFileList>(table);
	}

	OpenFileInfo GetFile(idx_t i) override {
		return i < data_files.size() ? data_files[i] : OpenFileInfo();
	}

	unique_ptr<DeleteFilter> GetDeleteFilter(ClientContext &context, idx_t i) {
		return metadata->GetDeleteFilter(context, data_file_numbers[i]);
	}

	MooncakeTable &table;
	optional_ptr<MooncakeTableMetadata> metadata;
	vector<OpenFileInfo> data_files;
	vector<uint32_t> data_file_numbers;
};

struct MooncakeFunctionInfo : public TableFunctionInfo {
	MooncakeFunctionInfo(MooncakeTable &_table) : table(_table) {
	}

	MooncakeTable &table;
};

struct MooncakeMultiFileReader : public MultiFileReader {
	MooncakeMultiFileReader(MooncakeTable &_table) : table(_table) {
	}

	static unique_ptr<MultiFileReader> Create(const TableFunction &table_function) {
		return make_uniq<MooncakeMultiFileReader>(table_function.function_info->Cast<MooncakeFunctionInfo>().table);
	}

	shared_ptr<MultiFileList> CreateFileList(ClientContext &, const vector<string> &, FileGlobOptions) override {
		return make_shared_ptr<MooncakeMultiFileList>(table);
	}

	bool Bind(MultiFileOptions &, MultiFileList &, vector<LogicalType> &return_types, vector<string> &names,
	          MultiFileReaderBindData &) override {
		for (auto &column : table.GetColumns().Logical()) {
			return_types.emplace_back(column.GetType());
			names.emplace_back(column.GetName());
		}
		return true;
	}

	ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind_data,
	                                      const vector<MultiFileColumnDefinition> &global_columns,
	                                      const vector<ColumnIndex> &global_column_ids,
	                                      optional_ptr<TableFilterSet> table_filters, ClientContext &context,
	                                      MultiFileGlobalState &gstate) override {
		auto &file_list = bind_data.Cast<MultiFileBindData>().file_list->Cast<MooncakeMultiFileList>();
		idx_t file_list_idx = reader_data.reader->file_list_idx.GetIndex();
		reader_data.reader->deletion_filter = file_list.GetDeleteFilter(context, file_list_idx);
		return MultiFileReader::InitializeReader(reader_data, bind_data, global_columns, global_column_ids,
		                                         table_filters, context, gstate);
	}

	MooncakeTable &table;
};

static TableFunction &GetParquetScan(ClientContext &context) {
	return ExtensionUtil::GetTableFunction(*context.db, "parquet_scan").functions.GetFunctionReferenceByOffset(0);
}

static unique_ptr<GlobalTableFunctionState> MooncakeScanInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	bind_data.file_list->Cast<MooncakeMultiFileList>().LazyInitialize(context, bind_data.names, input.column_ids,
	                                                                  input.filters);
	return GetParquetScan(context).init_global(context, input);
}

static InsertionOrderPreservingMap<string> MooncakeScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = input.table_function.function_info->Cast<MooncakeFunctionInfo>().table.name;
	return result;
}

static BindInfo MooncakeScanGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	return BindInfo(bind_data->Cast<MultiFileBindData>().file_list->Cast<MooncakeMultiFileList>().table);
}

TableFunction MooncakeTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	TableFunction mooncake_scan = GetParquetScan(context);
	mooncake_scan.name = "mooncake_scan";
	mooncake_scan.init_global = MooncakeScanInitGlobal;
	mooncake_scan.to_string = MooncakeScanToString;
	mooncake_scan.get_bind_info = MooncakeScanGetBindInfo;
	mooncake_scan.get_multi_file_reader = MooncakeMultiFileReader::Create;
	mooncake_scan.function_info = make_shared_ptr<MooncakeFunctionInfo>(*this);
	vector<Value> inputs {""};
	named_parameter_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	TableFunctionBindInput bind_input(inputs, named_parameters, input_table_types, input_table_names, nullptr /*info*/,
	                                  nullptr /*binder*/, mooncake_scan, {} /*ref*/);
	vector<LogicalType> return_types;
	vector<string> names;
	bind_data = mooncake_scan.bind(context, bind_input, return_types, names);
	return mooncake_scan;
}

} // namespace duckdb
