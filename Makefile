.PHONY: all build_cython clean lupdate lrelease

SRC_DIR     := src
BUILD_DIR   := build
LIB_DIR     := lib
INCLUDE_DIR := 3rdparty/duckdb/include

I18N_XML := resource/i18n/en_US.xml
I18N_TS  := resource/i18n/en_US.ts
I18N_QM  := resource/i18n/en_US.qm

DUCKDB_LIB := $(LIB_DIR)/libduckdb.so
CORE_LIB   := $(LIB_DIR)/libcore.so

SRC := $(wildcard $(SRC_DIR)/*.cxx)
OBJ := $(SRC:$(SRC_DIR)/%.cxx=$(BUILD_DIR)/%.o)

CXX := g++
CXXFLAGS := -fPIC -std=c++26 -O3 -march=native -flto -Wall -Wextra -Wno-unused-parameter
LDFLAGS  := -shared -flto=auto -Wl,-rpath,'$$ORIGIN'
LDLIBS   := -L$(LIB_DIR) -lduckdb

all: $(CORE_LIB) build_cython

$(CORE_LIB): $(OBJ) | $(DUCKDB_LIB)
	$(CXX) $^ -o $(BUILD_DIR)/libcore.so $(LDFLAGS) $(LDLIBS)
	cp $(BUILD_DIR)/libcore.so $@

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cxx | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -I$(INCLUDE_DIR) -c $< -o $@

$(DUCKDB_LIB): | $(LIB_DIR)
	cp 3rdparty/duckdb/lib/libduckdb.so $@

$(LIB_DIR):
	mkdir -p $@

$(BUILD_DIR):
	mkdir -p $@

build_cython:
	uv run setup.py build_ext --inplace

clean:
	rm -rf $(BUILD_DIR) $(LIB_DIR)

lupdate:
	@if [ -f "$(I18N_XML)" ]; then cp "$(I18N_XML)" "$(I18N_TS)"; fi
	/usr/lib64/qt6/bin/lupdate -no-obsolete -extensions py,pyx . -ts $(I18N_TS)
	mv $(I18N_TS) $(I18N_XML)

lrelease:
	cp $(I18N_XML) $(I18N_TS)
	/usr/lib64/qt6/bin/lrelease $(I18N_TS) -qm $(I18N_QM)
	rm -f $(I18N_TS)
