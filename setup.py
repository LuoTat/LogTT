from Cython.Build import cythonize
from Cython.Compiler import Options
from setuptools import Extension, setup

Options.docstrings = False

extensions = [
    Extension(
        name="modules.logparser.parsers",
        sources=["modules/logparser/parsers.pyx"],
        include_dirs=["3rdparty/duckdb/include", "src"],
        library_dirs=["lib"],
        libraries=["core"],
        runtime_library_dirs=["$ORIGIN/../../lib"],
        extra_compile_args=[
            "-Wall",
            "-O3",
            "-march=native",
            "-std=c++26",
        ],
        language="c++",
    ),
    Extension(
        name="modules.duckdb_service",
        sources=["modules/duckdb_service.pyx"],
        include_dirs=["3rdparty/duckdb/include", "src"],
        library_dirs=["lib"],
        libraries=["core"],
        runtime_library_dirs=["$ORIGIN/../lib"],
        extra_compile_args=[
            "-Wall",
            "-O3",
            "-march=native",
            "-std=c++26",
        ],
        language="c++",
    ),
    Extension(
        name="modules.log_analysis",
        sources=["modules/log_analysis.pyx"],
        include_dirs=["3rdparty/duckdb/include", "src"],
        library_dirs=["lib"],
        libraries=["core"],
        runtime_library_dirs=["$ORIGIN/../lib"],
        extra_compile_args=[
            "-Wall",
            "-O3",
            "-march=native",
            "-std=c++26",
            "-fopenmp",
        ],
        extra_link_args=["-fopenmp"],
        language="c++",
    ),
]

setup(
    packages=[],
    ext_modules=cythonize(
        extensions,
        show_all_warnings=True,
        annotate=True,
        compiler_directives={
            "boundscheck": False,
            "wraparound": False,
            "initializedcheck": False,
            "nonecheck": False,
            "overflowcheck": False,
            "cdivision": True,
            "cpow": True,
            "always_allow_keywords": False,
            "c_string_type": "unicode",
            "c_string_encoding": "utf8",
            "warn.undeclared": True,
            "warn.maybe_uninitialized": True,
            "warn.unused": True,
            "warn.unused_arg": True,
            "warn.unused_result": True,
            "show_performance_hints": True,
        },
    ),
)
