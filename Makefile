.PHONY: all build_lib build_cython clean

SRC_DIR   := modules/core/src
BUILD_DIR := build
LIB_DIR   := lib

TARGET    := $(BUILD_DIR)/libcore.so
LIB_TARGET:= $(LIB_DIR)/libcore.so

SRC := $(wildcard $(SRC_DIR)/*.cxx)
OBJ := $(SRC:$(SRC_DIR)/%.cxx=$(BUILD_DIR)/%.o)

CXX := g++
CXXFLAGS := -fPIC -std=c++26 -O3 -flto -Wall -Wextra -Wno-unused-parameter
LDFLAGS  := -shared -flto=auto -Wl,-rpath,'$$ORIGIN'
LDLIBS   := -L$(LIB_DIR) -lduckdb

all: build_lib

build_lib: $(LIB_TARGET)

$(LIB_TARGET): $(TARGET) | $(LIB_DIR)
	cp $< $@

$(TARGET): $(OBJ)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LDLIBS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cxx | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $@

$(LIB_DIR):
	mkdir -p $@

build_cython:
	uv run setup.py build_ext --inplace

clean:
	rm -rf $(BUILD_DIR) $(LIB_TARGET)