from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

ctypedef string Token
ctypedef vector[Token] Content

cdef object mask_log_df(object log_df, vector[pair[string, string]]& maskings)
cdef string mask_log(string log, vector[pair[string, string]]& maskings)
cdef object split_log_df(object log_df, vector[string]& delimiters)
cdef Content split_log(string log, vector[string]& delimiters)
cdef to_table(
    object log_df,
    vector[string]& log_templates,
    string structured_table_name,
    string templates_table_name,
    bint keep_para,
)
cdef object load_data(string log_file, string log_format)
