PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=mooncake
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

include extension-ci-tools/makefiles/duckdb_extension.Makefile

format-all:
	cd moonlink_ffi && cargo fmt
	find src -iname '*.hpp' -o -iname '*.cpp' | xargs clang-format -i
