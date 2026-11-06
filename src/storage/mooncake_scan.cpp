#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "storage/mooncake_table.hpp"
#include "storage/mooncake_table_metadata.hpp"

namespace duckdb {

struct DataFileStatistics : public ObjectCacheEntry {
public:
	DataFileStatistics(ClientContext &context, const string &data_file, const MultiFileBindData &bind_data) {
		auto options = bind_data.interface->InitializeOptions(context, nullptr);
		auto reader = bind_data.interface->CreateReader(context, OpenFileInfo(data_file), *options, bind_data.file_options);
		for (auto &name : bind_data.names) {
			column_stats[name] = reader->GetStatistics(context, name);
		}
	}

	static string ObjectType() {
		return "mooncake_data_file_statistics";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	optional_idx GetEstimatedCacheMemory() const override {
		return sizeof(*this) + column_stats.size() * (sizeof(BaseStatistics) + sizeof(Identifier) + 128);
	}

	identifier_map_t<unique_ptr<BaseStatistics>> column_stats;
};

struct MooncakeMultiFileList : public MultiFileList {
	MooncakeMultiFileList(MooncakeTable &_table)
	    : MultiFileList(), table(_table), metadata(), data_files(),
	      data_file_numbers() {
	}

	// Lazily initialized because pg_duckdb binds each query three times
	void LazyInitialize(ClientContext &context, const MultiFileBindData &bind_data, const vector<column_t> &column_ids,
	                    optional_ptr<TableFilterSet> filters) {
		metadata = &table.GetTableMetadata();
		for (uint32_t data_file_number = 0; data_file_number < metadata->GetNumDataFiles(); data_file_number++) {
			auto data_file = metadata->GetDataFile(data_file_number);
			if (filters) {
				auto &cache = ObjectCache::GetObjectCache(context);
				auto file_stats = cache.GetWithTypePrefix<DataFileStatistics>(data_file);
				if (!file_stats) {
					// Reading Parquet metadata also uses the object cache. Do not hold
					// its mutex while constructing this entry via GetOrCreate.
					file_stats = make_shared_ptr<DataFileStatistics>(context, data_file, bind_data);
					cache.PutWithTypePrefix<DataFileStatistics>(data_file, file_stats);
				}
				bool skip_file = false;
				for (auto &entry : *filters) {
					auto column_id = column_ids[entry.GetIndex().GetIndex()];
					if (IsVirtualColumn(column_id)) {
						continue;
					}
					auto &stats = file_stats->column_stats.at(bind_data.names[column_id]);
					auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "mooncake scan");
					if (stats && filter.CheckStatistics(context, *stats) == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
						skip_file = true;
						break;
					}
				}
				if (skip_file) {
					continue;
				}
			}
			data_files.push_back(data_file);
			data_file_numbers.push_back(data_file_number);
		}
	}

	vector<OpenFileInfo> GetAllFiles() const override {
		return data_files;
	}

	FileExpandResult GetExpandResult() const override {
		return FileExpandResult::MULTIPLE_FILES;
	}

	idx_t GetTotalFileCount() const override {
		return data_files.size();
	}

	unique_ptr<MultiFileList> Copy() const override {
		D_ASSERT(data_files.empty());
		return make_uniq<MooncakeMultiFileList>(table);
	}

	OpenFileInfo GetFile(idx_t i) const override {
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

	static unique_ptr<MultiFileReader> Create(const BoundTableFunction &table_function) {
		return make_uniq<MooncakeMultiFileReader>(table_function.function_info->Cast<MooncakeFunctionInfo>().table);
	}

	shared_ptr<MultiFileList> CreateFileList(ClientContext &, const vector<string> &, const FileGlobInput &) override {
		return make_shared_ptr<MooncakeMultiFileList>(table);
	}

	shared_ptr<MultiFileList> CreateFileList(ClientContext &, vector<OpenFileInfo>, const FileGlobInput &) override {
		return make_shared_ptr<MooncakeMultiFileList>(table);
	}

	unique_ptr<MultiFileReader> Copy() const override {
		return make_uniq<MooncakeMultiFileReader>(table);
	}

	bool Bind(MultiFileOptions &, MultiFileList &, vector<LogicalType> &return_types, vector<Identifier> &names,
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

static TableFunction GetParquetScan(ClientContext &context) {
	ExtensionLoader loader(*context.db, "mooncake");
	return *loader.GetTableFunction("parquet_scan").functions.GetFunctionByOffset(0);
}

static unique_ptr<GlobalTableFunctionState> MooncakeScanInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	bind_data.file_list->Cast<MooncakeMultiFileList>().LazyInitialize(context, bind_data, input.column_ids,
	                                                                  input.filters);
	return GetParquetScan(context).init_global(context, input);
}

static InsertionOrderPreservingMap<string> MooncakeScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = input.table_function.function_info->Cast<MooncakeFunctionInfo>().table.name.GetIdentifierName();
	return result;
}

static BindInfo MooncakeScanGetBindInfo(TableFunctionGetBindInfoInput &input) {
	return BindInfo(ScanType::EXTERNAL);
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
	named_argument_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<Identifier> input_table_names;
	BoundTableFunction bound_function(mooncake_scan);
	TableFunctionBindInput bind_input(inputs, named_parameters, input_table_types, input_table_names, nullptr /*info*/,
	                                  nullptr /*binder*/, bound_function, {} /*ref*/);
	vector<LogicalType> return_types;
	vector<Identifier> names;
	bind_data = mooncake_scan.bind(context, bind_input, return_types, names);
	return mooncake_scan;
}

} // namespace duckdb
