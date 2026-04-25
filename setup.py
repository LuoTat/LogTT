from Cython.Build import cythonize
from Cython.Compiler import Options
from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext

Options.docstrings = False
Options.fast_fail = True


class BuildExtWithCompilerFlags(build_ext):
    def build_extensions(self):
        compile_args = [
            "-DCYTHON_CLINE_IN_TRACEBACK=0",
            "-Wall",
            "-O3",
            "-march=native",
            "-std=c++26",
        ]

        for extension in self.extensions:
            extension.extra_compile_args.extend(compile_args)

        super().build_extensions()


extensions = [
    Extension(
        name="modules.logparser.parsers",
        sources=["modules/logparser/parsers.pyx"],
        include_dirs=["3rdparty/duckdb/include", "src"],
        library_dirs=["lib"],
        libraries=["core"],
        runtime_library_dirs=["$ORIGIN/../../lib"],
        language="c++",
    ),
    Extension(
        name="modules.duckdb_service",
        sources=["modules/duckdb_service.pyx"],
        include_dirs=["3rdparty/duckdb/include", "src"],
        library_dirs=["lib"],
        libraries=["core"],
        runtime_library_dirs=["$ORIGIN/../lib"],
        language="c++",
    ),
    Extension(
        name="modules.log_analysis",
        sources=["modules/log_analysis.pyx"],
        include_dirs=["3rdparty/duckdb/include", "src"],
        library_dirs=["lib"],
        libraries=["core"],
        runtime_library_dirs=["$ORIGIN/../lib"],
        extra_compile_args=["-fopenmp"],
        extra_link_args=["-fopenmp"],
        language="c++",
    ),
]

setup(
    packages=[],
    cmdclass={"build_ext": BuildExtWithCompilerFlags},
    ext_modules=cythonize(
        extensions,
        # force=True,
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
