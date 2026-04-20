from libc.stdint cimport int32_t, int64_t, uint32_t
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

from modules.log_analysis cimport (
    get_level_distribution as cxx_get_level_distribution,
    get_log_frequency_distribution as cxx_get_log_frequency_distribution,
    get_log_level_frequency_distribution as cxx_get_log_level_frequency_distribution,
)


cdef class LogAnalysis:
    @staticmethod
    def get_level_distribution(string table_name) -> tuple[list[str], list[int]]:
        cdef pair[vector[string], vector[uint32_t]] result

        with nogil:
            result = cxx_get_level_distribution(table_name)

        return result

    @staticmethod
    def get_log_frequency_distribution(string table_name, int32_t months, int32_t days, int64_t micros) -> tuple[list[int], list[int]]:
        cdef pair[vector[int64_t], vector[uint32_t]] result

        with nogil:
            result = cxx_get_log_frequency_distribution(table_name, months, days, micros)

        return result

    @staticmethod
    def get_log_level_frequency_distribution(string table_name, int32_t months, int32_t days, int64_t micros) -> dict[str, tuple[list[int], list[int]]]:
        cdef unordered_map[string, pair[vector[int64_t], vector[uint32_t]]] result

        with nogil:
            result = cxx_get_log_level_frequency_distribution(table_name, months, days, micros)

        return result
