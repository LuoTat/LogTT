from libc.stdint cimport int32_t, int64_t, uint32_t
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector
cimport numpy as cnp

from modules.log_analysis cimport (
    get_level_distribution as cxx_get_level_distribution,
    get_log_frequency_distribution as cxx_get_log_frequency_distribution,
    has_column as cxx_has_column,
)


cdef class LogAnalysis:
    @staticmethod
    def get_level_distribution(string table_name) -> list[tuple[str, int]]:
        cdef vector[pair[string, uint32_t]] result

        with nogil:
            result = cxx_get_level_distribution(table_name)

        return result

    @staticmethod
    def get_log_frequency_distribution(string table_name, int32_t months, int32_t days, int64_t micros) -> list[tuple[int, int]]:
        cdef vector[pair[int64_t, uint32_t]] result

        with nogil:
            result = cxx_get_log_frequency_distribution(table_name, months, days, micros)

        return result
