from Cython.Build import cythonize
from Cython.Compiler import Options
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext

Options.docstrings = False
Options.fast_fail = True

class BuildExtWithCompilerFlags(build_ext):
    def build_extensions(self):
        compiler_type = self.compiler.compiler_type

        if compiler_type == "msvc":
            compile_args = [
                "/DCYTHON_CLINE_IN_TRACEBACK=0",
                "/W4",
                "/O2",
                "/std:c++latest",
            ]
        else:
            compile_args = [
                "-DCYTHON_CLINE_IN_TRACEBACK=0",
                "-Wall",
                "-O3",
                "-std=c++26",
            ]

        for extension in self.extensions:
            extension.extra_compile_args = compile_args

        super().build_extensions()

extensions = [
    Extension(
        name="modules.logparser.core",
        sources=[
            "modules/logparser/py_drain_log_parser.pyx",
            "modules/logparser/utils.cxx",   # 额外的 C++ 文件
        ],
        language="c++",
        include_dirs=[
            "include",
        ],
        libraries=[
            "duckdb",
        ],
        library_dirs=[
            "lib",  # 替换为 DuckDB 库的实际路径
        ],
    )
]

setup(
    packages=[],
    cmdclass={"build_ext": BuildExtWithCompilerFlags},
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
            "profile": True,
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
