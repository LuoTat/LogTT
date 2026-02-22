from Cython.Build import cythonize
from Cython.Compiler import Options
from setuptools import Extension, setup

Options.docstrings = False
Options.fast_fail = True

extensions = [
    Extension(
        "modules.logparser.*",
        ["modules/logparser/*.pyx"],
        extra_compile_args=[
            "-DCYTHON_CLINE_IN_TRACEBACK=0",
            "-O3",
            "-std:c++20",
        ],
    ),
]

setup(
    packages=[],
    ext_modules=cythonize(
        extensions,
        force=True,
        show_all_warnings=True,
        annotate=True,
        compiler_directives={
            "boundscheck": False,
            "wraparound": False,
            "initializedcheck": False,
            "nonecheck": False,
            "overflowcheck": False,
            "cdivision": True,
            "cdivision_warnings": False,
            "cpow": True,
            "always_allow_keywords": False,
            "c_string_type": "unicode",
            "c_string_encoding": "utf8",
            # "profile": True,
            "optimize.use_switch": True,
            "optimize.unpack_method_calls": True,
            "warn.undeclared": True,
            "warn.unreachable": True,
            "warn.maybe_uninitialized": True,
            "warn.unused": True,
            "warn.unused_arg": True,
            "warn.unused_result": True,
            "show_performance_hints": True,
        },
    ),
)
