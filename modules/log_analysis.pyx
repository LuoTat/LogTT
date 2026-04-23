cdef object np

import numpy as np
from cython.parallel import prange
from libc.stdint cimport int32_t, int64_t
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

from modules.log_analysis cimport (
    get_level_distribution as cxx_get_level_distribution,
    get_log_frequency_distribution as cxx_get_log_frequency_distribution,
    get_log_level_frequency_distribution as cxx_get_log_level_frequency_distribution,
    get_template_frequency_distribution as cxx_get_template_frequency_distribution,
    get_template_transition_matrix as cxx_get_template_transition_matrix,
)


cdef class LogAnalysis:
    @staticmethod
    def get_level_distribution(string table_name) -> tuple[list[str], list[int]]:
        cdef pair[vector[string], vector[int64_t]] result

        with nogil:
            result = cxx_get_level_distribution(table_name)

        return result

    @staticmethod
    def get_log_frequency_distribution(string table_name, int32_t months, int32_t days, int64_t micros) -> tuple[np.ndarray, np.ndarray]:
        cdef pair[vector[int64_t], vector[int64_t]] cxx_result

        with nogil:
            cxx_result = cxx_get_log_frequency_distribution(table_name, months, days, micros)

        cdef object epochs = np.empty(cxx_result.first.size(), dtype=np.int64)
        cdef object counts = np.empty(cxx_result.second.size(), dtype=np.uint32)
        cdef int64_t [::1] epochs_view = epochs
        cdef int64_t [::1] counts_view = counts

        cdef size_t i
        with nogil:
            for i in prange(cxx_result.first.size()):
                epochs_view[i] = cxx_result.first[i]
                counts_view[i] = cxx_result.second[i]

        return epochs, counts

    @staticmethod
    def get_template_frequency_distribution(string table_name, int32_t months, int32_t days, int64_t micros) -> tuple[np.ndarray, np.ndarray]:
        cdef pair[vector[int64_t], vector[int64_t]] cxx_result

        with nogil:
            cxx_result = cxx_get_template_frequency_distribution(table_name, months, days, micros)

        cdef object epochs = np.empty(cxx_result.first.size(), dtype=np.int64)
        cdef object counts = np.empty(cxx_result.second.size(), dtype=np.uint32)
        cdef int64_t [::1] epochs_view = epochs
        cdef int64_t [::1] counts_view = counts

        cdef size_t i
        with nogil:
            for i in prange(cxx_result.first.size()):
                epochs_view[i] = cxx_result.first[i]
                counts_view[i] = cxx_result.second[i]

        return epochs, counts

    @staticmethod
    def get_log_level_frequency_distribution(string table_name, int32_t months, int32_t days, int64_t micros) -> dict[str, tuple[np.ndarray, np.ndarray]]:
        cdef unordered_map[string, pair[vector[int64_t], vector[int64_t]]] cxx_result

        with nogil:
            cxx_result = cxx_get_log_level_frequency_distribution(table_name, months, days, micros)

        cdef dict result = {}
        cdef pair[string, pair[vector[int64_t], vector[int64_t]]] pair
        cdef object epochs
        cdef object counts
        cdef int64_t [::1] epochs_view
        cdef int64_t [::1] counts_view
        cdef size_t i
        for pair in cxx_result:
            epochs = np.empty(pair.second.first.size(), dtype=np.int64)
            counts = np.empty(pair.second.second.size(), dtype=np.uint32)
            epochs_view = epochs
            counts_view = counts

            with nogil:
                for i in prange(pair.second.first.size()):
                    epochs_view[i] = pair.second.first[i]
                    counts_view[i] = pair.second.second[i]

            result[pair.first] = (epochs, counts)

        return result

    @staticmethod
    def get_template_transition_matrix(string structured_table_name, string template_table_name) -> np.ndarray:
        cdef pair[int64_t, vector[vector[int64_t]]] cxx_result

        with nogil:
            cxx_result = cxx_get_template_transition_matrix(structured_table_name, template_table_name)

        cdef int64_t dim = cxx_result.first
        cdef object matrix = np.zeros((dim, dim), dtype=np.uint32)
        cdef int64_t [:,::1] matrix_view = matrix

        cdef size_t i
        with nogil:
            for i in prange(cxx_result.second.size()):
                matrix_view[cxx_result.second[i][0]][cxx_result.second[i][1]] = cxx_result.second[i][2]

        return matrix