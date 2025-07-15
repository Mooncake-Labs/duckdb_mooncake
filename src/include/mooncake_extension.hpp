#pragma once

#include "duckdb/main/extension.hpp"

namespace duckdb {

class MooncakeExtension : public Extension {
public:
	void Load(DuckDB &db) override;

	string Name() override;

	string Version() const override;
};

} // namespace duckdb
