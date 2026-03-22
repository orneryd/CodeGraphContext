# src/codegraphcontext/tools/graph_builder.py
import asyncio
import json
import traceback
import pathspec
from pathlib import Path
from time import perf_counter
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple
from datetime import datetime

from ..core.database import DatabaseManager
from ..core.jobs import JobManager, JobStatus
from ..utils.debug_log import debug_log, info_logger, error_logger, warning_logger

# New imports for tree-sitter (using tree-sitter-language-pack)
from tree_sitter import Language, Parser
from ..utils.tree_sitter_manager import get_tree_sitter_manager
from ..cli.config_manager import get_config_value
from .indexing_scalability import (
    IndexingMetrics,
    SymbolIndex,
    collect_call_edges,
    collect_inheritance_edges,
    normalize_args_for_storage,
    normalize_parameter_name,
)


class TreeSitterParser:
    """A generic parser wrapper for a specific language using tree-sitter."""

    def __init__(self, language_name: str):
        self.language_name = language_name
        self.ts_manager = get_tree_sitter_manager()

        # Get the language (cached) and create a new parser for this instance
        self.language: Language = self.ts_manager.get_language_safe(language_name)
        # In tree-sitter 0.25+, Parser takes language in constructor
        self.parser = Parser(self.language)

        self.language_specific_parser = None
        if self.language_name == "python":
            from .languages.python import PythonTreeSitterParser

            self.language_specific_parser = PythonTreeSitterParser(self)
        elif self.language_name == "javascript":
            from .languages.javascript import JavascriptTreeSitterParser

            self.language_specific_parser = JavascriptTreeSitterParser(self)
        elif self.language_name == "go":
            from .languages.go import GoTreeSitterParser

            self.language_specific_parser = GoTreeSitterParser(self)
        elif self.language_name == "typescript":
            from .languages.typescript import TypescriptTreeSitterParser

            self.language_specific_parser = TypescriptTreeSitterParser(self)
        elif self.language_name == "cpp":
            from .languages.cpp import CppTreeSitterParser

            self.language_specific_parser = CppTreeSitterParser(self)
        elif self.language_name == "rust":
            from .languages.rust import RustTreeSitterParser

            self.language_specific_parser = RustTreeSitterParser(self)
        elif self.language_name == "c":
            from .languages.c import CTreeSitterParser

            self.language_specific_parser = CTreeSitterParser(self)
        elif self.language_name == "java":
            from .languages.java import JavaTreeSitterParser

            self.language_specific_parser = JavaTreeSitterParser(self)
        elif self.language_name == "ruby":
            from .languages.ruby import RubyTreeSitterParser

            self.language_specific_parser = RubyTreeSitterParser(self)
        elif self.language_name == "c_sharp":
            from .languages.csharp import CSharpTreeSitterParser

            self.language_specific_parser = CSharpTreeSitterParser(self)
        elif self.language_name == "php":
            from .languages.php import PhpTreeSitterParser

            self.language_specific_parser = PhpTreeSitterParser(self)
        elif self.language_name == "kotlin":
            from .languages.kotlin import KotlinTreeSitterParser

            self.language_specific_parser = KotlinTreeSitterParser(self)
        elif self.language_name == "scala":
            from .languages.scala import ScalaTreeSitterParser

            self.language_specific_parser = ScalaTreeSitterParser(self)
        elif self.language_name == "swift":
            from .languages.swift import SwiftTreeSitterParser

            self.language_specific_parser = SwiftTreeSitterParser(self)
        elif self.language_name == "haskell":
            from .languages.haskell import HaskellTreeSitterParser

            self.language_specific_parser = HaskellTreeSitterParser(self)
        elif self.language_name == "dart":
            from .languages.dart import DartTreeSitterParser

            self.language_specific_parser = DartTreeSitterParser(self)
        elif self.language_name == "perl":
            from .languages.perl import PerlTreeSitterParser

            self.language_specific_parser = PerlTreeSitterParser(self)
        elif self.language_name == "elixir":
            from .languages.elixir import ElixirTreeSitterParser

            self.language_specific_parser = ElixirTreeSitterParser(self)

    def parse(self, path: Path, is_dependency: bool = False, **kwargs) -> Dict:
        """Dispatches parsing to the language-specific parser."""
        if self.language_specific_parser:
            return self.language_specific_parser.parse(path, is_dependency, **kwargs)
        else:
            raise NotImplementedError(
                f"No language-specific parser implemented for {self.language_name}"
            )


class GraphBuilder:
    """Module for building and managing the Neo4j code graph."""

    def __init__(
        self,
        db_manager: DatabaseManager,
        job_manager: JobManager,
        loop: asyncio.AbstractEventLoop,
    ):
        self.db_manager = db_manager
        self.job_manager = job_manager
        self.loop = loop
        self.driver = self.db_manager.get_driver()
        self._query_timing_stats: Dict[str, Dict[str, Any]] = {}
        self._row_size_cache: Dict[int, int] = {}
        self._write_progress_mode = "batch"
        self.parsers = {
            ".py": TreeSitterParser("python"),
            ".ipynb": TreeSitterParser("python"),
            ".js": TreeSitterParser("javascript"),
            ".jsx": TreeSitterParser("javascript"),
            ".mjs": TreeSitterParser("javascript"),
            ".cjs": TreeSitterParser("javascript"),
            ".go": TreeSitterParser("go"),
            ".ts": TreeSitterParser("typescript"),
            ".tsx": TreeSitterParser("typescript"),
            ".cpp": TreeSitterParser("cpp"),
            ".h": TreeSitterParser("cpp"),
            ".hpp": TreeSitterParser("cpp"),
            ".hh": TreeSitterParser("cpp"),
            ".rs": TreeSitterParser("rust"),
            ".c": TreeSitterParser("c"),
            # '.h': TreeSitterParser('c'), # Need to write an algo for distinguishing C vs C++ headers
            ".java": TreeSitterParser("java"),
            ".rb": TreeSitterParser("ruby"),
            ".cs": TreeSitterParser("c_sharp"),
            ".php": TreeSitterParser("php"),
            ".kt": TreeSitterParser("kotlin"),
            ".scala": TreeSitterParser("scala"),
            ".sc": TreeSitterParser("scala"),
            ".swift": TreeSitterParser("swift"),
            ".hs": TreeSitterParser("haskell"),
            ".dart": TreeSitterParser("dart"),
            ".pl": TreeSitterParser("perl"),
            ".pm": TreeSitterParser("perl"),
            ".ex": TreeSitterParser("elixir"),
            ".exs": TreeSitterParser("elixir"),
        }
        self.create_schema()

    # A general schema creation based on common features across languages
    def create_schema(self):
        """Create constraints and indexes in Neo4j."""
        # When adding a new node type with a unique key, add its constraint here.
        with self.driver.session() as session:
            try:
                session.run(
                    "CREATE CONSTRAINT repository_path IF NOT EXISTS FOR (r:Repository) REQUIRE r.path IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT path IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT directory_path IF NOT EXISTS FOR (d:Directory) REQUIRE d.path IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT function_unique IF NOT EXISTS FOR (f:Function) REQUIRE (f.name, f.path, f.line_number) IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT class_unique IF NOT EXISTS FOR (c:Class) REQUIRE (c.name, c.path, c.line_number) IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT trait_unique IF NOT EXISTS FOR (t:Trait) REQUIRE (t.name, t.path, t.line_number) IS UNIQUE"
                )  # Added trait constraint
                session.run(
                    "CREATE CONSTRAINT interface_unique IF NOT EXISTS FOR (i:Interface) REQUIRE (i.name, i.path, i.line_number) IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT macro_unique IF NOT EXISTS FOR (m:Macro) REQUIRE (m.name, m.path, m.line_number) IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT variable_unique IF NOT EXISTS FOR (v:Variable) REQUIRE (v.name, v.path, v.line_number) IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT module_name IF NOT EXISTS FOR (m:Module) REQUIRE m.name IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT struct_cpp IF NOT EXISTS FOR (cstruct: Struct) REQUIRE (cstruct.name, cstruct.path, cstruct.line_number) IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT enum_cpp IF NOT EXISTS FOR (cenum: Enum) REQUIRE (cenum.name, cenum.path, cenum.line_number) IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT union_cpp IF NOT EXISTS FOR (cunion: Union) REQUIRE (cunion.name, cunion.path, cunion.line_number) IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT annotation_unique IF NOT EXISTS FOR (a:Annotation) REQUIRE (a.name, a.path, a.line_number) IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT record_unique IF NOT EXISTS FOR (r:Record) REQUIRE (r.name, r.path, r.line_number) IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT property_unique IF NOT EXISTS FOR (p:Property) REQUIRE (p.name, p.path, p.line_number) IS UNIQUE"
                )
                session.run(
                    "CREATE CONSTRAINT parameter_unique IF NOT EXISTS FOR (p:Parameter) REQUIRE (p.name, p.path, p.function_line_number) IS UNIQUE"
                )

                # Indexes for language attribute
                session.run(
                    "CREATE INDEX function_lang IF NOT EXISTS FOR (f:Function) ON (f.lang)"
                )
                session.run(
                    "CREATE INDEX class_lang IF NOT EXISTS FOR (c:Class) ON (c.lang)"
                )
                session.run(
                    "CREATE INDEX annotation_lang IF NOT EXISTS FOR (a:Annotation) ON (a.lang)"
                )
                session.run(
                    "CREATE INDEX function_name IF NOT EXISTS FOR (f:Function) ON (f.name)"
                )
                session.run(
                    "CREATE INDEX class_name IF NOT EXISTS FOR (c:Class) ON (c.name)"
                )
                is_falkordb = (
                    getattr(self.db_manager, "get_backend_type", lambda: "neo4j")()
                    != "neo4j"
                )
                if is_falkordb:
                    # FalkorDB uses db.idx.fulltext.createNodeIndex per label
                    for label in ["Function", "Class"]:
                        try:
                            session.run(
                                f"CALL db.idx.fulltext.createNodeIndex('{label}', 'name', 'source', 'docstring')"
                            )
                        except Exception:
                            pass  # Index may already exist
                else:
                    session.run("""
                        CREATE FULLTEXT INDEX code_search_index IF NOT EXISTS
                        FOR (n:Function|Class|Variable)
                        ON EACH [n.name, n.source, n.docstring]
                    """)

                info_logger("Database schema verified/created successfully")
            except Exception as e:
                warning_logger(f"Schema creation warning: {e}")

    def _pre_scan_for_imports(self, files: list[Path]) -> dict:
        """Dispatches pre-scan to the correct language-specific implementation."""
        imports_map = {}

        # Group files by language/extension
        files_by_lang = {}
        for file in files:
            if file.suffix in self.parsers:
                lang_ext = file.suffix
                if lang_ext not in files_by_lang:
                    files_by_lang[lang_ext] = []
                files_by_lang[lang_ext].append(file)

        if ".py" in files_by_lang:
            from .languages import python as python_lang_module

            imports_map.update(
                python_lang_module.pre_scan_python(
                    files_by_lang[".py"], self.parsers[".py"]
                )
            )
        if ".ipynb" in files_by_lang:
            from .languages import python as python_lang_module

            imports_map.update(
                python_lang_module.pre_scan_python(
                    files_by_lang[".ipynb"], self.parsers[".ipynb"]
                )
            )
        if ".js" in files_by_lang:
            from .languages import javascript as js_lang_module

            imports_map.update(
                js_lang_module.pre_scan_javascript(
                    files_by_lang[".js"], self.parsers[".js"]
                )
            )
        if ".jsx" in files_by_lang:
            from .languages import javascript as js_lang_module

            imports_map.update(
                js_lang_module.pre_scan_javascript(
                    files_by_lang[".jsx"], self.parsers[".jsx"]
                )
            )
        if ".mjs" in files_by_lang:
            from .languages import javascript as js_lang_module

            imports_map.update(
                js_lang_module.pre_scan_javascript(
                    files_by_lang[".mjs"], self.parsers[".mjs"]
                )
            )
        if ".cjs" in files_by_lang:
            from .languages import javascript as js_lang_module

            imports_map.update(
                js_lang_module.pre_scan_javascript(
                    files_by_lang[".cjs"], self.parsers[".cjs"]
                )
            )
        if ".go" in files_by_lang:
            from .languages import go as go_lang_module

            imports_map.update(
                go_lang_module.pre_scan_go(files_by_lang[".go"], self.parsers[".go"])
            )
        if ".ts" in files_by_lang:
            from .languages import typescript as ts_lang_module

            imports_map.update(
                ts_lang_module.pre_scan_typescript(
                    files_by_lang[".ts"], self.parsers[".ts"]
                )
            )
        if ".tsx" in files_by_lang:
            from .languages import typescriptjsx as tsx_lang_module

            imports_map.update(
                tsx_lang_module.pre_scan_typescript(
                    files_by_lang[".tsx"], self.parsers[".tsx"]
                )
            )
        if ".cpp" in files_by_lang:
            from .languages import cpp as cpp_lang_module

            imports_map.update(
                cpp_lang_module.pre_scan_cpp(
                    files_by_lang[".cpp"], self.parsers[".cpp"]
                )
            )
        if ".h" in files_by_lang:
            from .languages import cpp as cpp_lang_module

            imports_map.update(
                cpp_lang_module.pre_scan_cpp(files_by_lang[".h"], self.parsers[".h"])
            )
        if ".hpp" in files_by_lang:
            from .languages import cpp as cpp_lang_module

            imports_map.update(
                cpp_lang_module.pre_scan_cpp(
                    files_by_lang[".hpp"], self.parsers[".hpp"]
                )
            )
        if ".hh" in files_by_lang:
            from .languages import cpp as cpp_lang_module

            imports_map.update(
                cpp_lang_module.pre_scan_cpp(files_by_lang[".hh"], self.parsers[".hh"])
            )
        if ".rs" in files_by_lang:
            from .languages import rust as rust_lang_module

            imports_map.update(
                rust_lang_module.pre_scan_rust(
                    files_by_lang[".rs"], self.parsers[".rs"]
                )
            )
        if ".c" in files_by_lang:
            from .languages import c as c_lang_module

            imports_map.update(
                c_lang_module.pre_scan_c(files_by_lang[".c"], self.parsers[".c"])
            )
        elif ".java" in files_by_lang:
            from .languages import java as java_lang_module

            imports_map.update(
                java_lang_module.pre_scan_java(
                    files_by_lang[".java"], self.parsers[".java"]
                )
            )
        elif ".rb" in files_by_lang:
            from .languages import ruby as ruby_lang_module

            imports_map.update(
                ruby_lang_module.pre_scan_ruby(
                    files_by_lang[".rb"], self.parsers[".rb"]
                )
            )
        elif ".cs" in files_by_lang:
            from .languages import csharp as csharp_lang_module

            imports_map.update(
                csharp_lang_module.pre_scan_csharp(
                    files_by_lang[".cs"], self.parsers[".cs"]
                )
            )
        if ".kt" in files_by_lang:
            from .languages import kotlin as kotlin_lang_module

            imports_map.update(
                kotlin_lang_module.pre_scan_kotlin(
                    files_by_lang[".kt"], self.parsers[".kt"]
                )
            )
        if ".scala" in files_by_lang:
            from .languages import scala as scala_lang_module

            imports_map.update(
                scala_lang_module.pre_scan_scala(
                    files_by_lang[".scala"], self.parsers[".scala"]
                )
            )
        if ".sc" in files_by_lang:
            from .languages import scala as scala_lang_module

            imports_map.update(
                scala_lang_module.pre_scan_scala(
                    files_by_lang[".sc"], self.parsers[".sc"]
                )
            )
        if ".swift" in files_by_lang:
            from .languages import swift as swift_lang_module

            imports_map.update(
                swift_lang_module.pre_scan_swift(
                    files_by_lang[".swift"], self.parsers[".swift"]
                )
            )
        if ".dart" in files_by_lang:
            from .languages import dart as dart_lang_module

            imports_map.update(
                dart_lang_module.pre_scan_dart(
                    files_by_lang[".dart"], self.parsers[".dart"]
                )
            )
        if ".pl" in files_by_lang:
            from .languages import perl as perl_lang_module

            imports_map.update(
                perl_lang_module.pre_scan_perl(
                    files_by_lang[".pl"], self.parsers[".pl"]
                )
            )
        if ".pm" in files_by_lang:
            from .languages import perl as perl_lang_module

            imports_map.update(
                perl_lang_module.pre_scan_perl(
                    files_by_lang[".pm"], self.parsers[".pm"]
                )
            )
        if ".ex" in files_by_lang:
            from .languages import elixir as elixir_lang_module

            imports_map.update(
                elixir_lang_module.pre_scan_elixir(
                    files_by_lang[".ex"], self.parsers[".ex"]
                )
            )
        if ".exs" in files_by_lang:
            from .languages import elixir as elixir_lang_module

            imports_map.update(
                elixir_lang_module.pre_scan_elixir(
                    files_by_lang[".exs"], self.parsers[".exs"]
                )
            )

        return imports_map

    # Language-agnostic method
    def add_repository_to_graph(self, repo_path: Path, is_dependency: bool = False):
        """Adds a repository node using its absolute path as the unique key."""
        repo_name = repo_path.name
        repo_path_str = str(repo_path.resolve())
        with self.driver.session() as session:
            session.run(
                """
                MERGE (r:Repository {path: $path})
                SET r.name = $name, r.is_dependency = $is_dependency
                """,
                path=repo_path_str,
                name=repo_name,
                is_dependency=is_dependency,
            )

    def _get_batch_target_bytes(self) -> int:
        return 131072

    def _get_max_index_file_bytes(self) -> int:
        return 32768

    def _reset_row_size_cache(self):
        self._row_size_cache = {}

    def _filter_oversized_files(self, files: list[Path]) -> tuple[list[Path], list[Path]]:
        max_file_bytes = self._get_max_index_file_bytes()
        kept_files: list[Path] = []
        skipped_files: list[Path] = []
        for file_path in files:
            try:
                if file_path.stat().st_size > max_file_bytes:
                    skipped_files.append(file_path)
                    continue
            except OSError:
                skipped_files.append(file_path)
                continue
            kept_files.append(file_path)
        return kept_files, skipped_files

    def _get_large_repo_threshold(self) -> int:
        raw_value = get_config_value("GLOBAL_FALLBACK_FILE_THRESHOLD") or "400"
        try:
            return max(50, int(raw_value))
        except ValueError:
            return 400

    def _fast_mode_enabled(self) -> bool:
        return (get_config_value("FAST_INDEX_MODE") or "false").lower() == "true"

    def _global_fallback_enabled(self) -> bool:
        positive_flag = get_config_value("ENABLE_GLOBAL_FALLBACK_RESOLUTION")
        if positive_flag is not None:
            return positive_flag.lower() == "true"
        legacy_flag = get_config_value("SKIP_EXTERNAL_RESOLUTION")
        if legacy_flag is not None:
            return legacy_flag.lower() != "true"
        return False

    def _update_job_phase(self, job_id: Optional[str], phase: str):
        if job_id:
            self.job_manager.update_job(job_id, current_phase=phase)

    def _reset_job_progress(
        self,
        job_id: Optional[str],
        phase: str,
        total_units: int,
        current_item: str = "",
        progress_unit: str = "files",
        progress_detail: Optional[str] = None,
        total_batches: int = 0,
    ):
        if job_id:
            self.job_manager.update_job(
                job_id,
                current_phase=phase,
                total_files=max(total_units, 1),
                processed_files=0,
                current_file=current_item,
                progress_unit=progress_unit,
                progress_detail=progress_detail,
                current_batch=0,
                total_batches=total_batches,
            )

    def _estimate_batch_row_size(self, row: Dict[str, Any]) -> int:
        cache = getattr(self, "_row_size_cache", None)
        if cache is None:
            cache = {}
            self._row_size_cache = cache
        cache_key = id(row)
        cached_size = cache.get(cache_key)
        if cached_size is not None:
            return cached_size
        try:
            size = max(
                1, len(json.dumps(row, sort_keys=True, default=str).encode("utf-8"))
            )
        except TypeError:
            size = max(1, len(str(row).encode("utf-8")))
        cache[cache_key] = size
        return size

    def _iter_sized_batches(self, rows: list[Dict[str, Any]]):
        if not rows:
            return
        target_bytes = self._get_batch_target_bytes()
        current_batch: list[Dict[str, Any]] = []
        current_size = 0
        for row in rows:
            row_size = self._estimate_batch_row_size(row)
            if current_batch and current_size + row_size > target_bytes:
                yield current_batch
                current_batch = []
                current_size = 0
            current_batch.append(row)
            current_size += row_size
        if current_batch:
            yield current_batch

    def _count_batched_chunks(self, rows: list[Dict[str, Any]]) -> int:
        return sum(1 for _ in self._iter_sized_batches(rows))

    def _count_file_batch_chunks(self, rows: Dict[str, Any]) -> int:
        total_chunks = 0
        for value in rows.values():
            if isinstance(value, list):
                total_chunks += self._count_batched_chunks(value)
            elif isinstance(value, dict):
                total_chunks += sum(
                    self._count_batched_chunks(label_rows)
                    for label_rows in value.values()
                )
        return total_chunks

    def _format_progress_detail(
        self,
        label: str,
        processed_units: int,
        total_units: int,
        progress_unit: str,
        batch_index: int,
        total_batches: int,
    ) -> str:
        detail = f"{label}: {processed_units}/{max(total_units, 1)} {progress_unit}"
        if total_batches > 0:
            detail += f" | batch {batch_index}/{total_batches}"
        return detail

    def _supports_unwind_batch_queries(self) -> bool:
        # Neo4j-compatible backends support parameterized UNWIND batches and we retain
        # the fallback path on per-query failure. Kuzu/FalkorDB variants still require
        # the portable per-row path.
        db_manager = getattr(self, "db_manager", None)
        if db_manager is None:
            return False
        backend_type = getattr(db_manager, "get_backend_type", lambda: "neo4j")()
        return backend_type == "neo4j"

    def _truncate_debug_value(self, value: Any, limit: int = 400) -> str:
        text = json.dumps(value, sort_keys=True, default=str)
        if len(text) <= limit:
            return text
        return text[: limit - 3] + "..."

    def _format_query_failure(
        self,
        exc: Exception,
        query: str,
        row: Optional[Dict[str, Any]] = None,
    ) -> RuntimeError:
        query_preview = " ".join(query.split())
        if len(query_preview) > 240:
            query_preview = query_preview[:237] + "..."

        message = [
            f"Graph write failed: {type(exc).__name__}: {exc}",
            f"Query: {query_preview}",
        ]
        if row is not None:
            message.append(f"Row: {self._truncate_debug_value(row)}")
        return RuntimeError("\n".join(message))

    def _format_batch_query_failure(
        self,
        exc: Exception,
        query: str,
        chunk: list[Dict[str, Any]],
    ) -> RuntimeError:
        first_row = chunk[0] if chunk else None
        error = self._format_query_failure(exc, query, first_row)
        return RuntimeError(f"{error}\nBatch rows: {len(chunk)}")

    def _should_raise_batch_failure(self) -> bool:
        db_manager = getattr(self, "db_manager", None)
        if db_manager is None:
            return False
        return getattr(db_manager, "get_backend_type", lambda: None)() == "neo4j"

    def _reset_query_timings(self):
        self._query_timing_stats = {}

    def _record_query_timing(
        self, query: str, duration_seconds: float, row_count: int, mode: str
    ):
        if not hasattr(self, "_query_timing_stats"):
            self._query_timing_stats = {}
        query_preview = " ".join(query.split())
        if len(query_preview) > 240:
            query_preview = query_preview[:237] + "..."

        key = f"{mode}:{query_preview}"
        stats = self._query_timing_stats.setdefault(
            key,
            {
                "mode": mode,
                "query": query_preview,
                "calls": 0,
                "rows": 0,
                "total_seconds": 0.0,
                "max_seconds": 0.0,
            },
        )
        stats["calls"] += 1
        stats["rows"] += row_count
        stats["total_seconds"] += duration_seconds
        stats["max_seconds"] = max(stats["max_seconds"], duration_seconds)
        debug_log(
            "QUERY_TIMING "
            f"mode={mode} rows={row_count} duration={duration_seconds:.4f}s "
            f"query={query_preview}"
        )

    def _query_timing_summary(self) -> list[Dict[str, Any]]:
        summary: list[Dict[str, Any]] = []
        for stats in self._query_timing_stats.values():
            calls = int(stats["calls"])
            total_seconds = float(stats["total_seconds"])
            summary.append(
                {
                    "mode": stats["mode"],
                    "calls": calls,
                    "rows": int(stats["rows"]),
                    "total_seconds": round(total_seconds, 4),
                    "avg_seconds": round(total_seconds / calls, 4) if calls else 0.0,
                    "max_seconds": round(float(stats["max_seconds"]), 4),
                    "query": stats["query"],
                }
            )
        summary.sort(key=lambda item: item["total_seconds"], reverse=True)
        return summary

    def _run_row_fallback_chunk(
        self,
        session,
        chunk: list[Dict],
        fallback,
        query: str,
        progress_callback: Optional[Callable[[int], None]] = None,
    ):
        previous_mode = getattr(self, "_write_progress_mode", "batch")
        self._write_progress_mode = "fallback"
        if hasattr(session, "execute_write"):

            def write_chunk(tx):
                for row in chunk:
                    query_start = perf_counter()
                    try:
                        result = fallback(tx, row)
                    except Exception as exc:
                        raise self._format_query_failure(exc, query, row) from exc
                    if hasattr(result, "consume"):
                        result.consume()
                    self._record_query_timing(
                        query,
                        perf_counter() - query_start,
                        1,
                        "fallback-row",
                    )
                    if progress_callback is not None:
                        progress_callback(1)

            try:
                session.execute_write(write_chunk)
                return
            finally:
                self._write_progress_mode = previous_mode

        try:
            for row in chunk:
                query_start = perf_counter()
                try:
                    result = fallback(session, row)
                except Exception as exc:
                    raise self._format_query_failure(exc, query, row) from exc
                if hasattr(result, "consume"):
                    result.consume()
                self._record_query_timing(
                    query,
                    perf_counter() - query_start,
                    1,
                    "fallback-row",
                )
                if progress_callback is not None:
                    progress_callback(1)
        finally:
            self._write_progress_mode = previous_mode

    async def _run_row_fallback_chunk_async(
        self,
        session,
        chunk: list[Dict],
        fallback,
        query: str,
        progress_callback: Optional[Callable[[int], None]] = None,
    ):
        previous_mode = getattr(self, "_write_progress_mode", "batch")
        self._write_progress_mode = "fallback"
        try:
            for index, row in enumerate(chunk, start=1):
                query_start = perf_counter()
                try:
                    result = fallback(session, row)
                except Exception as exc:
                    raise self._format_query_failure(exc, query, row) from exc
                if hasattr(result, "consume"):
                    result.consume()
                self._record_query_timing(
                    query,
                    perf_counter() - query_start,
                    1,
                    "fallback-row",
                )
                if progress_callback is not None:
                    progress_callback(1)
                if index % 25 == 0:
                    await asyncio.sleep(0)
        finally:
            self._write_progress_mode = previous_mode

    def _run_batched_query(
        self,
        session,
        query: str,
        rows: list[Dict],
        fallback=None,
        progress_callback: Optional[Callable[[int], None]] = None,
    ):
        if not rows:
            return
        for chunk in self._iter_sized_batches(rows):
            if not self._supports_unwind_batch_queries():
                if fallback is None:
                    raise ValueError(
                        "Fallback query is required when UNWIND batching is disabled"
                    )
                self._run_row_fallback_chunk(
                    session,
                    chunk,
                    fallback,
                    query,
                    progress_callback=progress_callback,
                )
                continue
            try:
                query_start = perf_counter()
                result = session.run(query, rows=chunk)
                if hasattr(result, "consume"):
                    result.consume()
                self._record_query_timing(
                    query,
                    perf_counter() - query_start,
                    len(chunk),
                    "batch",
                )
                if progress_callback is not None:
                    progress_callback(len(chunk))
            except Exception as exc:
                if fallback is None:
                    raise
                if self._should_raise_batch_failure():
                    raise self._format_batch_query_failure(exc, query, chunk) from exc
                self._run_row_fallback_chunk(
                    session,
                    chunk,
                    fallback,
                    query,
                    progress_callback=progress_callback,
                )

    async def _run_batched_query_async(
        self,
        session,
        query: str,
        rows: list[Dict],
        fallback=None,
        progress_callback: Optional[Callable[[int], None]] = None,
    ):
        if not rows:
            return
        for chunk in self._iter_sized_batches(rows):
            if not self._supports_unwind_batch_queries():
                if fallback is None:
                    raise ValueError(
                        "Fallback query is required when UNWIND batching is disabled"
                    )
                await self._run_row_fallback_chunk_async(
                    session,
                    chunk,
                    fallback,
                    query,
                    progress_callback=progress_callback,
                )
            else:
                try:
                    query_start = perf_counter()
                    result = session.run(query, rows=chunk)
                    if hasattr(result, "consume"):
                        result.consume()
                    self._record_query_timing(
                        query,
                        perf_counter() - query_start,
                        len(chunk),
                        "batch",
                    )
                except Exception as exc:
                    if fallback is None:
                        raise
                    if self._should_raise_batch_failure():
                        raise self._format_batch_query_failure(exc, query, chunk) from exc
                    await self._run_row_fallback_chunk_async(
                        session,
                        chunk,
                        fallback,
                        query,
                        progress_callback=progress_callback,
                    )
            if progress_callback is not None:
                if self._supports_unwind_batch_queries():
                    progress_callback(len(chunk))
            await asyncio.sleep(0)

    def _count_file_batch_rows(self, rows: Dict[str, Any]) -> int:
        total_rows = 0
        for value in rows.values():
            if isinstance(value, list):
                total_rows += len(value)
            elif isinstance(value, dict):
                total_rows += sum(len(label_rows) for label_rows in value.values())
        return total_rows

    def _freeze_batch_row_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return tuple(
                (key, self._freeze_batch_row_value(item))
                for key, item in sorted(value.items())
            )
        if isinstance(value, (list, tuple)):
            return tuple(self._freeze_batch_row_value(item) for item in value)
        return value

    def _prepare_file_batch_rows_with_stats(
        self, file_batch: list[Dict]
    ) -> tuple[Dict[str, Any], int]:
        rows: Dict[str, Any] = {
            "files": [],
            "directories": [],
            "repo_to_dir": [],
            "dir_to_dir": [],
            "repo_to_file": [],
            "dir_to_file": [],
            "nodes": {},
            "parameters": [],
            "modules": [],
            "imports": [],
            "nested_function_contains": [],
            "class_contains": [],
            "module_inclusions": [],
        }
        seen_rows: Dict[str, Any] = {
            "files": set(),
            "directories": set(),
            "repo_to_dir": set(),
            "dir_to_dir": set(),
            "repo_to_file": set(),
            "dir_to_file": set(),
            "nodes": {},
            "parameters": set(),
            "modules": set(),
            "imports": set(),
            "nested_function_contains": set(),
            "class_contains": set(),
            "module_inclusions": set(),
        }
        total_rows = 0

        def append_unique(bucket: str, row: Dict[str, Any], label: Optional[str] = None):
            nonlocal total_rows
            row_key = self._freeze_batch_row_value(row)
            if label is None:
                bucket_seen = seen_rows[bucket]
                if row_key in bucket_seen:
                    return
                bucket_seen.add(row_key)
                rows[bucket].append(row)
            else:
                label_seen = seen_rows["nodes"].setdefault(label, set())
                if row_key in label_seen:
                    return
                label_seen.add(row_key)
                rows["nodes"].setdefault(label, []).append(row)
            total_rows += 1

        item_mappings = [
            ("Function", "functions"),
            ("Class", "classes"),
            ("Trait", "traits"),
            ("Variable", "variables"),
            ("Interface", "interfaces"),
            ("Macro", "macros"),
            ("Struct", "structs"),
            ("Enum", "enums"),
            ("Union", "unions"),
            ("Record", "records"),
            ("Property", "properties"),
        ]
        for file_data in file_batch:
            file_path_obj = Path(file_data["path"]).resolve()
            repo_path_obj = Path(file_data["repo_path"]).resolve()
            file_path = str(file_path_obj)
            repo_path = str(repo_path_obj)
            file_name = file_path_obj.name
            is_dependency = file_data.get("is_dependency", False)
            try:
                relative_path = str(file_path_obj.relative_to(repo_path_obj))
            except ValueError:
                relative_path = file_name

            append_unique(
                "files",
                {
                    "path": file_path,
                    "name": file_name,
                    "relative_path": relative_path,
                    "is_dependency": is_dependency,
                },
            )

            relative_path_obj = Path(relative_path)
            parent_path_obj = repo_path_obj
            for part in relative_path_obj.parts[:-1]:
                current_path_obj = parent_path_obj / part
                current_path = str(current_path_obj)
                append_unique("directories", {"path": current_path, "name": part})
                if parent_path_obj == repo_path_obj:
                    append_unique(
                        "repo_to_dir",
                        {"repo_path": repo_path, "dir_path": current_path},
                    )
                else:
                    append_unique(
                        "dir_to_dir",
                        {
                            "parent_path": str(parent_path_obj),
                            "dir_path": current_path,
                        },
                    )
                parent_path_obj = current_path_obj

            if parent_path_obj == repo_path_obj:
                append_unique(
                    "repo_to_file",
                    {"repo_path": repo_path, "file_path": file_path}
                )
            else:
                append_unique(
                    "dir_to_file",
                    {"dir_path": str(parent_path_obj), "file_path": file_path},
                )

            for label, key in item_mappings:
                for item in file_data.get(key, []):
                    item_props = dict(item)
                    item_props["path"] = file_path
                    function_args = item.get("args", []) if label == "Function" else []
                    if label == "Function":
                        item_props["args"] = normalize_args_for_storage(function_args)
                    if (
                        label == "Function"
                        and "cyclomatic_complexity" not in item_props
                    ):
                        item_props["cyclomatic_complexity"] = 1
                    append_unique(
                        "nodes",
                        {
                            "file_path": file_path,
                            "name": item["name"],
                            "line_number": item["line_number"],
                            "props": item_props,
                        }
                        ,
                        label=label,
                    )
                    if label == "Function":
                        for arg_name in function_args:
                            append_unique(
                                "parameters",
                                {
                                    "func_name": item["name"],
                                    "path": file_path,
                                    "function_line_number": item["line_number"],
                                    "name": normalize_parameter_name(arg_name),
                                },
                            )

            for module in file_data.get("modules", []):
                append_unique(
                    "modules",
                    {
                        "name": module["name"],
                        "lang": file_data.get("lang"),
                    },
                )

            for item in file_data.get("functions", []):
                if item.get("context_type") == "function_definition":
                    append_unique(
                        "nested_function_contains",
                        {
                            "path": file_path,
                            "outer_name": item.get("context"),
                            "inner_name": item["name"],
                            "inner_line_number": item["line_number"],
                        },
                    )
                if item.get("class_context"):
                    append_unique(
                        "class_contains",
                        {
                            "path": file_path,
                            "class_name": item["class_context"],
                            "func_name": item["name"],
                            "func_line": item["line_number"],
                        },
                    )

            for imp in file_data.get("imports", []):
                module_name = (
                    imp.get("source")
                    if file_data.get("lang") == "javascript"
                    else imp.get("name")
                )
                if not module_name:
                    continue
                rel_props = {}
                if imp.get("alias"):
                    rel_props["alias"] = imp["alias"]
                if imp.get("line_number"):
                    rel_props["line_number"] = imp["line_number"]
                if imp.get("full_import_name"):
                    rel_props["full_import_name"] = imp["full_import_name"]
                if imp.get("name"):
                    rel_props["imported_name"] = imp.get("name", "*")
                append_unique(
                    "imports",
                    {
                        "path": file_path,
                        "module_name": module_name,
                        "lang": file_data.get("lang"),
                        "module_props": {
                            "lang": file_data.get("lang"),
                            "full_import_name": imp.get("full_import_name"),
                        },
                        "rel_props": rel_props,
                    },
                )

            for inclusion in file_data.get("module_inclusions", []):
                append_unique(
                    "module_inclusions",
                    {
                        "path": file_path,
                        "class_name": inclusion["class"],
                        "module_name": inclusion["module"],
                    },
                )
        return rows, total_rows

    def _prepare_file_batch_rows(self, file_batch: list[Dict]) -> Dict[str, Any]:
        rows, _total_rows = self._prepare_file_batch_rows_with_stats(file_batch)
        return rows

    def _build_batched_node_write_query(self, label: str) -> str:
        return f"""
            UNWIND $rows AS row
            WITH row.file_path AS file_path, row.name AS name, row.line_number AS line_number, row.props AS props
            MATCH (f:File {{path: file_path}})
            MERGE (n:{label} {{name: name, path: file_path, line_number: line_number}})
            SET n += props
            MERGE (f)-[:CONTAINS]->(n)
            """

    def _write_file_batch_rows(
        self,
        session,
        rows: Dict[str, Any],
        metrics: Optional[IndexingMetrics] = None,
        progress_callback: Optional[Callable[[str, int], None]] = None,
    ):
        def advance(label: str) -> Callable[[int], None]:
            if progress_callback is None:
                return lambda _count: None
            return lambda count: progress_callback(label, count)

        self._run_batched_query(
            session,
            """
            UNWIND $rows AS row
            MERGE (f:File {path: row.path})
            SET f.name = row.name, f.relative_path = row.relative_path, f.is_dependency = row.is_dependency
            """,
            rows["files"],
            fallback=lambda current_session, row: current_session.run(
                """
                MERGE (f:File {path: $path})
                SET f.name = $name, f.relative_path = $relative_path, f.is_dependency = $is_dependency
                """,
                **row,
            ),
            progress_callback=advance("files"),
        )
        self._run_batched_query(
            session,
            """
            UNWIND $rows AS row
            MERGE (d:Directory {path: row.path})
            SET d.name = row.name
            """,
            rows["directories"],
            fallback=lambda current_session, row: current_session.run(
                "MERGE (d:Directory {path: $path}) SET d.name = $name",
                **row,
            ),
            progress_callback=advance("directories"),
        )
        self._run_batched_query(
            session,
            """
            UNWIND $rows AS row
            MATCH (r:Repository {path: row.repo_path})
            MATCH (d:Directory {path: row.dir_path})
            MERGE (r)-[:CONTAINS]->(d)
            """,
            rows["repo_to_dir"],
            fallback=lambda current_session, row: current_session.run(
                "MATCH (r:Repository {path: $repo_path}) MATCH (d:Directory {path: $dir_path}) MERGE (r)-[:CONTAINS]->(d)",
                **row,
            ),
            progress_callback=advance("repo directories"),
        )
        self._run_batched_query(
            session,
            """
            UNWIND $rows AS row
            MATCH (parent:Directory {path: row.parent_path})
            MATCH (d:Directory {path: row.dir_path})
            MERGE (parent)-[:CONTAINS]->(d)
            """,
            rows["dir_to_dir"],
            fallback=lambda current_session, row: current_session.run(
                "MATCH (parent:Directory {path: $parent_path}) MATCH (d:Directory {path: $dir_path}) MERGE (parent)-[:CONTAINS]->(d)",
                **row,
            ),
            progress_callback=advance("nested directories"),
        )
        self._run_batched_query(
            session,
            """
            UNWIND $rows AS row
            MATCH (r:Repository {path: row.repo_path})
            MATCH (f:File {path: row.file_path})
            MERGE (r)-[:CONTAINS]->(f)
            """,
            rows["repo_to_file"],
            fallback=lambda current_session, row: current_session.run(
                "MATCH (r:Repository {path: $repo_path}) MATCH (f:File {path: $file_path}) MERGE (r)-[:CONTAINS]->(f)",
                **row,
            ),
            progress_callback=advance("repository files"),
        )
        self._run_batched_query(
            session,
            """
            UNWIND $rows AS row
            MATCH (d:Directory {path: row.dir_path})
            MATCH (f:File {path: row.file_path})
            MERGE (d)-[:CONTAINS]->(f)
            """,
            rows["dir_to_file"],
            fallback=lambda current_session, row: current_session.run(
                "MATCH (d:Directory {path: $dir_path}) MATCH (f:File {path: $file_path}) MERGE (d)-[:CONTAINS]->(f)",
                **row,
            ),
            progress_callback=advance("directory files"),
        )
        for label, label_rows in rows["nodes"].items():
            self._run_batched_query(
                session,
                self._build_batched_node_write_query(label),
                label_rows,
                fallback=lambda current_session, row, graph_label=label: current_session.run(
                    f"""
                    MATCH (f:File {{path: $file_path}})
                    MERGE (n:{graph_label} {{name: $name, path: $file_path, line_number: $line_number}})
                    SET n += $props
                    MERGE (f)-[:CONTAINS]->(n)
                    """,
                    **row,
                ),
                progress_callback=advance(label.lower()),
            )
        self._run_batched_query(
            session,
            """
            UNWIND $rows AS row
            MATCH (fn:Function {name: row.func_name, path: row.path, line_number: row.function_line_number})
            MERGE (p:Parameter {name: row.name, path: row.path, function_line_number: row.function_line_number})
            MERGE (fn)-[:HAS_PARAMETER]->(p)
            """,
            rows["parameters"],
            fallback=lambda current_session, row: current_session.run(
                """
                MATCH (fn:Function {name: $func_name, path: $path, line_number: $function_line_number})
                MERGE (p:Parameter {name: $name, path: $path, function_line_number: $function_line_number})
                MERGE (fn)-[:HAS_PARAMETER]->(p)
                """,
                **row,
            ),
            progress_callback=advance("parameters"),
        )
        self._run_batched_query(
            session,
            """
            UNWIND $rows AS row
            MERGE (mod:Module {name: row.name})
            ON CREATE SET mod.lang = row.lang
            ON MATCH SET mod.lang = COALESCE(mod.lang, row.lang)
            """,
            rows["modules"],
            fallback=lambda current_session, row: current_session.run(
                """
                MERGE (mod:Module {name: $name})
                ON CREATE SET mod.lang = $lang
                ON MATCH SET mod.lang = COALESCE(mod.lang, $lang)
                """,
                **row,
            ),
            progress_callback=advance("modules"),
        )
        self._run_batched_query(
            session,
            """
            UNWIND $rows AS row
            MATCH (outer:Function {name: row.outer_name, path: row.path})
            MATCH (inner:Function {name: row.inner_name, path: row.path, line_number: row.inner_line_number})
            MERGE (outer)-[:CONTAINS]->(inner)
            """,
            rows["nested_function_contains"],
            fallback=lambda current_session, row: current_session.run(
                """
                MATCH (outer:Function {name: $outer_name, path: $path})
                MATCH (inner:Function {name: $inner_name, path: $path, line_number: $inner_line_number})
                MERGE (outer)-[:CONTAINS]->(inner)
                """,
                **row,
            ),
            progress_callback=advance("nested functions"),
        )
        self._run_batched_query(
            session,
            """
            UNWIND $rows AS row
            MATCH (c:Class {name: row.class_name, path: row.path})
            MATCH (fn:Function {name: row.func_name, path: row.path, line_number: row.func_line})
            MERGE (c)-[:CONTAINS]->(fn)
            """,
            rows["class_contains"],
            fallback=lambda current_session, row: current_session.run(
                """
                MATCH (c:Class {name: $class_name, path: $path})
                MATCH (fn:Function {name: $func_name, path: $path, line_number: $func_line})
                MERGE (c)-[:CONTAINS]->(fn)
                """,
                **row,
            ),
            progress_callback=advance("class members"),
        )
        self._run_batched_query(
            session,
            """
            UNWIND $rows AS row
            MERGE (m:Module {name: row.module_name})
            SET m.lang = COALESCE(m.lang, row.lang)
            WITH row, m
            MATCH (f:File {path: row.path})
            MERGE (f)-[r:IMPORTS]->(m)
            SET r += row.rel_props
            """,
            rows["imports"],
            fallback=lambda current_session, row: current_session.run(
                """
                MATCH (f:File {path: $path})
                MERGE (m:Module {name: $module_name})
                SET m.lang = COALESCE(m.lang, $lang)
                MERGE (f)-[r:IMPORTS]->(m)
                SET r += $rel_props
                """,
                **row,
            ),
            progress_callback=advance("imports"),
        )
        self._run_batched_query(
            session,
            """
            UNWIND $rows AS row
            MATCH (c:Class {name: row.class_name, path: row.path})
            MERGE (m:Module {name: row.module_name})
            MERGE (c)-[:INCLUDES]->(m)
            """,
            rows["module_inclusions"],
            fallback=lambda current_session, row: current_session.run(
                """
                MATCH (c:Class {name: $class_name, path: $path})
                MERGE (m:Module {name: $module_name})
                MERGE (c)-[:INCLUDES]->(m)
                """,
                **row,
            ),
            progress_callback=advance("module inclusions"),
        )
        if metrics is not None:
            metrics.increment("batched_file_rows", len(rows["files"]))
            metrics.increment(
                "batched_node_rows",
                sum(len(label_rows) for label_rows in rows["nodes"].values()),
            )
            metrics.increment("batched_parameter_rows", len(rows["parameters"]))

    async def _write_file_batch_rows_async(
        self,
        session,
        rows: Dict[str, Any],
        metrics: Optional[IndexingMetrics] = None,
        progress_callback: Optional[Callable[[str, int], None]] = None,
    ):
        def advance(label: str) -> Callable[[int], None]:
            if progress_callback is None:
                return lambda _count: None
            return lambda count: progress_callback(label, count)

        await self._run_batched_query_async(
            session,
            """
            UNWIND $rows AS row
            MERGE (f:File {path: row.path})
            SET f.name = row.name, f.relative_path = row.relative_path, f.is_dependency = row.is_dependency
            """,
            rows["files"],
            fallback=lambda current_session, row: current_session.run(
                """
                MERGE (f:File {path: $path})
                SET f.name = $name, f.relative_path = $relative_path, f.is_dependency = $is_dependency
                """,
                **row,
            ),
            progress_callback=advance("files"),
        )
        await self._run_batched_query_async(
            session,
            """
            UNWIND $rows AS row
            MERGE (d:Directory {path: row.path})
            SET d.name = row.name
            """,
            rows["directories"],
            fallback=lambda current_session, row: current_session.run(
                "MERGE (d:Directory {path: $path}) SET d.name = $name",
                **row,
            ),
            progress_callback=advance("directories"),
        )
        await self._run_batched_query_async(
            session,
            """
            UNWIND $rows AS row
            MATCH (r:Repository {path: row.repo_path})
            MATCH (d:Directory {path: row.dir_path})
            MERGE (r)-[:CONTAINS]->(d)
            """,
            rows["repo_to_dir"],
            fallback=lambda current_session, row: current_session.run(
                "MATCH (r:Repository {path: $repo_path}) MATCH (d:Directory {path: $dir_path}) MERGE (r)-[:CONTAINS]->(d)",
                **row,
            ),
            progress_callback=advance("repo directories"),
        )
        await self._run_batched_query_async(
            session,
            """
            UNWIND $rows AS row
            MATCH (parent:Directory {path: row.parent_path})
            MATCH (d:Directory {path: row.dir_path})
            MERGE (parent)-[:CONTAINS]->(d)
            """,
            rows["dir_to_dir"],
            fallback=lambda current_session, row: current_session.run(
                "MATCH (parent:Directory {path: $parent_path}) MATCH (d:Directory {path: $dir_path}) MERGE (parent)-[:CONTAINS]->(d)",
                **row,
            ),
            progress_callback=advance("nested directories"),
        )
        await self._run_batched_query_async(
            session,
            """
            UNWIND $rows AS row
            MATCH (r:Repository {path: row.repo_path})
            MATCH (f:File {path: row.file_path})
            MERGE (r)-[:CONTAINS]->(f)
            """,
            rows["repo_to_file"],
            fallback=lambda current_session, row: current_session.run(
                "MATCH (r:Repository {path: $repo_path}) MATCH (f:File {path: $file_path}) MERGE (r)-[:CONTAINS]->(f)",
                **row,
            ),
            progress_callback=advance("repository files"),
        )
        await self._run_batched_query_async(
            session,
            """
            UNWIND $rows AS row
            MATCH (d:Directory {path: row.dir_path})
            MATCH (f:File {path: row.file_path})
            MERGE (d)-[:CONTAINS]->(f)
            """,
            rows["dir_to_file"],
            fallback=lambda current_session, row: current_session.run(
                "MATCH (d:Directory {path: $dir_path}) MATCH (f:File {path: $file_path}) MERGE (d)-[:CONTAINS]->(f)",
                **row,
            ),
            progress_callback=advance("directory files"),
        )
        for label, label_rows in rows["nodes"].items():
            await self._run_batched_query_async(
                session,
                self._build_batched_node_write_query(label),
                label_rows,
                fallback=lambda current_session, row, graph_label=label: current_session.run(
                    f"""
                    MATCH (f:File {{path: $file_path}})
                    MERGE (n:{graph_label} {{name: $name, path: $file_path, line_number: $line_number}})
                    SET n += $props
                    MERGE (f)-[:CONTAINS]->(n)
                    """,
                    **row,
                ),
                progress_callback=advance(label.lower()),
            )
        await self._run_batched_query_async(
            session,
            """
            UNWIND $rows AS row
            MATCH (fn:Function {name: row.func_name, path: row.path, line_number: row.function_line_number})
            MERGE (p:Parameter {name: row.name, path: row.path, function_line_number: row.function_line_number})
            MERGE (fn)-[:HAS_PARAMETER]->(p)
            """,
            rows["parameters"],
            fallback=lambda current_session, row: current_session.run(
                """
                MATCH (fn:Function {name: $func_name, path: $path, line_number: $function_line_number})
                MERGE (p:Parameter {name: $name, path: $path, function_line_number: $function_line_number})
                MERGE (fn)-[:HAS_PARAMETER]->(p)
                """,
                **row,
            ),
            progress_callback=advance("parameters"),
        )
        await self._run_batched_query_async(
            session,
            """
            UNWIND $rows AS row
            MERGE (mod:Module {name: row.name})
            ON CREATE SET mod.lang = row.lang
            ON MATCH SET mod.lang = COALESCE(mod.lang, row.lang)
            """,
            rows["modules"],
            fallback=lambda current_session, row: current_session.run(
                """
                MERGE (mod:Module {name: $name})
                ON CREATE SET mod.lang = $lang
                ON MATCH SET mod.lang = COALESCE(mod.lang, $lang)
                """,
                **row,
            ),
            progress_callback=advance("modules"),
        )
        await self._run_batched_query_async(
            session,
            """
            UNWIND $rows AS row
            MATCH (outer:Function {name: row.outer_name, path: row.path})
            MATCH (inner:Function {name: row.inner_name, path: row.path, line_number: row.inner_line_number})
            MERGE (outer)-[:CONTAINS]->(inner)
            """,
            rows["nested_function_contains"],
            fallback=lambda current_session, row: current_session.run(
                """
                MATCH (outer:Function {name: $outer_name, path: $path})
                MATCH (inner:Function {name: $inner_name, path: $path, line_number: $inner_line_number})
                MERGE (outer)-[:CONTAINS]->(inner)
                """,
                **row,
            ),
            progress_callback=advance("nested functions"),
        )
        await self._run_batched_query_async(
            session,
            """
            UNWIND $rows AS row
            MATCH (c:Class {name: row.class_name, path: row.path})
            MATCH (fn:Function {name: row.func_name, path: row.path, line_number: row.func_line})
            MERGE (c)-[:CONTAINS]->(fn)
            """,
            rows["class_contains"],
            fallback=lambda current_session, row: current_session.run(
                """
                MATCH (c:Class {name: $class_name, path: $path})
                MATCH (fn:Function {name: $func_name, path: $path, line_number: $func_line})
                MERGE (c)-[:CONTAINS]->(fn)
                """,
                **row,
            ),
            progress_callback=advance("class members"),
        )
        await self._run_batched_query_async(
            session,
            """
            UNWIND $rows AS row
            MERGE (m:Module {name: row.module_name})
            SET m.lang = COALESCE(m.lang, row.lang)
            WITH row, m
            MATCH (f:File {path: row.path})
            MERGE (f)-[r:IMPORTS]->(m)
            SET r += row.rel_props
            """,
            rows["imports"],
            fallback=lambda current_session, row: current_session.run(
                """
                MATCH (f:File {path: $path})
                MERGE (m:Module {name: $module_name})
                SET m.lang = COALESCE(m.lang, $lang)
                MERGE (f)-[r:IMPORTS]->(m)
                SET r += $rel_props
                """,
                **row,
            ),
            progress_callback=advance("imports"),
        )
        await self._run_batched_query_async(
            session,
            """
            UNWIND $rows AS row
            MATCH (c:Class {name: row.class_name, path: row.path})
            MERGE (m:Module {name: row.module_name})
            MERGE (c)-[:INCLUDES]->(m)
            """,
            rows["module_inclusions"],
            fallback=lambda current_session, row: current_session.run(
                """
                MATCH (c:Class {name: $class_name, path: $path})
                MERGE (m:Module {name: $module_name})
                MERGE (c)-[:INCLUDES]->(m)
                """,
                **row,
            ),
            progress_callback=advance("module inclusions"),
        )
        if metrics is not None:
            metrics.increment("batched_file_rows", len(rows["files"]))
            metrics.increment(
                "batched_node_rows",
                sum(len(label_rows) for label_rows in rows["nodes"].values()),
            )
            metrics.increment("batched_parameter_rows", len(rows["parameters"]))

    def add_file_to_graph(self, file_data: Dict, repo_name: str, imports_map: dict):
        calls_count = len(file_data.get("function_calls", []))
        debug_log(
            f"Executing add_file_to_graph for {file_data.get('path', 'unknown')} - Calls found: {calls_count}"
        )
        self._add_files_to_graph_batched([file_data])

    def _add_files_to_graph_batched(
        self,
        file_batch: list[Dict],
        metrics: Optional[IndexingMetrics] = None,
        progress_callback: Optional[Callable[[str, int], None]] = None,
    ):
        if not file_batch:
            return
        rows = self._prepare_file_batch_rows(file_batch)
        with self.driver.session() as session:
            self._write_file_batch_rows(
                session, rows, metrics=metrics, progress_callback=progress_callback
            )

    # Second pass to create relationships that depend on all files being present like call functions and class inheritance
    def _safe_run_create(self, session, query, params) -> bool:
        """Helper to run a creation query safely, catching exceptions and checking result."""
        try:
            result = session.run(query, **params)
            row = result.single()
            return row is not None and row.get("created", 0) > 0
        except Exception as e:
            # Optionally log, but suppress to allow fallback
            return False

    def _write_call_edges(
        self,
        session,
        edges,
        metrics: Optional[IndexingMetrics] = None,
        progress_callback: Optional[Callable[[int], None]] = None,
    ):
        grouped: Dict[tuple[str, str], list[Dict]] = {}
        for edge in edges:
            grouped.setdefault((edge.caller_label, edge.callee_label), []).append(
                {
                    "caller_name": edge.caller_name,
                    "caller_path": edge.caller_path,
                    "caller_line_number": edge.caller_line_number,
                    "callee_name": edge.callee_name,
                    "callee_path": edge.callee_path,
                    "callee_line_number": edge.callee_line_number,
                    "line_number": edge.line_number,
                    "args": edge.args,
                    "full_call_name": edge.full_call_name,
                }
            )

        query_map = {
            ("Function", "Function"): """
                UNWIND $rows AS row
                MATCH (caller:Function {name: row.caller_name, path: row.caller_path, line_number: row.caller_line_number})
                MATCH (callee:Function {name: row.callee_name, path: row.callee_path, line_number: row.callee_line_number})
                MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(callee)
            """,
            ("Function", "Class"): """
                UNWIND $rows AS row
                MATCH (caller:Function {name: row.caller_name, path: row.caller_path, line_number: row.caller_line_number})
                MATCH (callee:Class {name: row.callee_name, path: row.callee_path, line_number: row.callee_line_number})
                MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(callee)
            """,
            ("Class", "Function"): """
                UNWIND $rows AS row
                MATCH (caller:Class {name: row.caller_name, path: row.caller_path, line_number: row.caller_line_number})
                MATCH (callee:Function {name: row.callee_name, path: row.callee_path, line_number: row.callee_line_number})
                MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(callee)
            """,
            ("Class", "Class"): """
                UNWIND $rows AS row
                MATCH (caller:Class {name: row.caller_name, path: row.caller_path, line_number: row.caller_line_number})
                MATCH (callee:Class {name: row.callee_name, path: row.callee_path, line_number: row.callee_line_number})
                MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(callee)
            """,
            ("File", "Function"): """
                UNWIND $rows AS row
                MATCH (caller:File {path: row.caller_path})
                MATCH (callee:Function {name: row.callee_name, path: row.callee_path, line_number: row.callee_line_number})
                MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(callee)
            """,
            ("File", "Class"): """
                UNWIND $rows AS row
                MATCH (caller:File {path: row.caller_path})
                MATCH (callee:Class {name: row.callee_name, path: row.callee_path, line_number: row.callee_line_number})
                MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(callee)
            """,
        }
        fallback_map = {
            ("Function", "Function"): lambda current_session, row: current_session.run(
                """
                MATCH (caller:Function {name: $caller_name, path: $caller_path, line_number: $caller_line_number})
                MATCH (callee:Function {name: $callee_name, path: $callee_path, line_number: $callee_line_number})
                MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(callee)
                """,
                **row,
            ),
            ("Function", "Class"): lambda current_session, row: current_session.run(
                """
                MATCH (caller:Function {name: $caller_name, path: $caller_path, line_number: $caller_line_number})
                MATCH (callee:Class {name: $callee_name, path: $callee_path, line_number: $callee_line_number})
                MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(callee)
                """,
                **row,
            ),
            ("Class", "Function"): lambda current_session, row: current_session.run(
                """
                MATCH (caller:Class {name: $caller_name, path: $caller_path, line_number: $caller_line_number})
                MATCH (callee:Function {name: $callee_name, path: $callee_path, line_number: $callee_line_number})
                MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(callee)
                """,
                **row,
            ),
            ("Class", "Class"): lambda current_session, row: current_session.run(
                """
                MATCH (caller:Class {name: $caller_name, path: $caller_path, line_number: $caller_line_number})
                MATCH (callee:Class {name: $callee_name, path: $callee_path, line_number: $callee_line_number})
                MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(callee)
                """,
                **row,
            ),
            ("File", "Function"): lambda current_session, row: current_session.run(
                """
                MATCH (caller:File {path: $caller_path})
                MATCH (callee:Function {name: $callee_name, path: $callee_path, line_number: $callee_line_number})
                MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(callee)
                """,
                **row,
            ),
            ("File", "Class"): lambda current_session, row: current_session.run(
                """
                MATCH (caller:File {path: $caller_path})
                MATCH (callee:Class {name: $callee_name, path: $callee_path, line_number: $callee_line_number})
                MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(callee)
                """,
                **row,
            ),
        }
        for key, rows in grouped.items():
            query = query_map.get(key)
            if not query:
                continue
            self._run_batched_query(
                session,
                query,
                rows,
                fallback=fallback_map[key],
                progress_callback=progress_callback,
            )
        if metrics is not None:
            metrics.increment("created_call_edges", len(edges))

    async def _write_call_edges_async(
        self,
        session,
        edges,
        metrics: Optional[IndexingMetrics] = None,
        progress_callback: Optional[Callable[[int], None]] = None,
    ):
        grouped: Dict[tuple[str, str], list[Dict]] = {}
        for edge in edges:
            grouped.setdefault((edge.caller_label, edge.callee_label), []).append(
                {
                    "caller_name": edge.caller_name,
                    "caller_path": edge.caller_path,
                    "caller_line_number": edge.caller_line_number,
                    "callee_name": edge.callee_name,
                    "callee_path": edge.callee_path,
                    "callee_line_number": edge.callee_line_number,
                    "line_number": edge.line_number,
                    "args": edge.args,
                    "full_call_name": edge.full_call_name,
                }
            )

        query_map = {
            ("Function", "Function"): """
                UNWIND $rows AS row
                MATCH (caller:Function {name: row.caller_name, path: row.caller_path, line_number: row.caller_line_number})
                MATCH (callee:Function {name: row.callee_name, path: row.callee_path, line_number: row.callee_line_number})
                MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(callee)
            """,
            ("Function", "Class"): """
                UNWIND $rows AS row
                MATCH (caller:Function {name: row.caller_name, path: row.caller_path, line_number: row.caller_line_number})
                MATCH (callee:Class {name: row.callee_name, path: row.callee_path, line_number: row.callee_line_number})
                MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(callee)
            """,
            ("Class", "Function"): """
                UNWIND $rows AS row
                MATCH (caller:Class {name: row.caller_name, path: row.caller_path, line_number: row.caller_line_number})
                MATCH (callee:Function {name: row.callee_name, path: row.callee_path, line_number: row.callee_line_number})
                MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(callee)
            """,
            ("Class", "Class"): """
                UNWIND $rows AS row
                MATCH (caller:Class {name: row.caller_name, path: row.caller_path, line_number: row.caller_line_number})
                MATCH (callee:Class {name: row.callee_name, path: row.callee_path, line_number: row.callee_line_number})
                MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(callee)
            """,
            ("File", "Function"): """
                UNWIND $rows AS row
                MATCH (caller:File {path: row.caller_path})
                MATCH (callee:Function {name: row.callee_name, path: row.callee_path, line_number: row.callee_line_number})
                MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(callee)
            """,
            ("File", "Class"): """
                UNWIND $rows AS row
                MATCH (caller:File {path: row.caller_path})
                MATCH (callee:Class {name: row.callee_name, path: row.callee_path, line_number: row.callee_line_number})
                MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(callee)
            """,
        }
        fallback_map = {
            ("Function", "Function"): lambda current_session, row: current_session.run(
                """
                MATCH (caller:Function {name: $caller_name, path: $caller_path, line_number: $caller_line_number})
                MATCH (callee:Function {name: $callee_name, path: $callee_path, line_number: $callee_line_number})
                MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(callee)
                """,
                **row,
            ),
            ("Function", "Class"): lambda current_session, row: current_session.run(
                """
                MATCH (caller:Function {name: $caller_name, path: $caller_path, line_number: $caller_line_number})
                MATCH (callee:Class {name: $callee_name, path: $callee_path, line_number: $callee_line_number})
                MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(callee)
                """,
                **row,
            ),
            ("Class", "Function"): lambda current_session, row: current_session.run(
                """
                MATCH (caller:Class {name: $caller_name, path: $caller_path, line_number: $caller_line_number})
                MATCH (callee:Function {name: $callee_name, path: $callee_path, line_number: $callee_line_number})
                MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(callee)
                """,
                **row,
            ),
            ("Class", "Class"): lambda current_session, row: current_session.run(
                """
                MATCH (caller:Class {name: $caller_name, path: $caller_path, line_number: $caller_line_number})
                MATCH (callee:Class {name: $callee_name, path: $callee_path, line_number: $callee_line_number})
                MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(callee)
                """,
                **row,
            ),
            ("File", "Function"): lambda current_session, row: current_session.run(
                """
                MATCH (caller:File {path: $caller_path})
                MATCH (callee:Function {name: $callee_name, path: $callee_path, line_number: $callee_line_number})
                MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(callee)
                """,
                **row,
            ),
            ("File", "Class"): lambda current_session, row: current_session.run(
                """
                MATCH (caller:File {path: $caller_path})
                MATCH (callee:Class {name: $callee_name, path: $callee_path, line_number: $callee_line_number})
                MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(callee)
                """,
                **row,
            ),
        }
        for key, rows in grouped.items():
            query = query_map.get(key)
            if not query:
                continue
            await self._run_batched_query_async(
                session,
                query,
                rows,
                fallback=fallback_map[key],
                progress_callback=progress_callback,
            )
        if metrics is not None:
            metrics.increment("created_call_edges", len(edges))

    def _write_type_edges(
        self,
        session,
        edges,
        metrics: Optional[IndexingMetrics] = None,
        progress_callback: Optional[Callable[[int], None]] = None,
    ):
        grouped: Dict[tuple[str, str, str], list[Dict]] = {}
        for edge in edges:
            grouped.setdefault(
                (edge.relation, edge.child_label, edge.parent_label), []
            ).append(
                {
                    "child_name": edge.child_name,
                    "child_path": edge.child_path,
                    "child_line_number": edge.child_line_number,
                    "parent_name": edge.parent_name,
                    "parent_path": edge.parent_path,
                    "parent_line_number": edge.parent_line_number,
                }
            )
        for (relation, child_label, parent_label), rows in grouped.items():
            query = f"""
                UNWIND $rows AS row
                MATCH (child:{child_label} {{name: row.child_name, path: row.child_path, line_number: row.child_line_number}})
                MATCH (parent:{parent_label} {{name: row.parent_name, path: row.parent_path, line_number: row.parent_line_number}})
                MERGE (child)-[:{relation}]->(parent)
            """
            self._run_batched_query(
                session,
                query,
                rows,
                fallback=lambda current_session, row, edge_relation=relation, edge_child=child_label, edge_parent=parent_label: current_session.run(
                    f"""
                    MATCH (child:{edge_child} {{name: $child_name, path: $child_path, line_number: $child_line_number}})
                    MATCH (parent:{edge_parent} {{name: $parent_name, path: $parent_path, line_number: $parent_line_number}})
                    MERGE (child)-[:{edge_relation}]->(parent)
                    """,
                    **row,
                ),
                progress_callback=progress_callback,
            )
        if metrics is not None:
            metrics.increment("created_type_edges", len(edges))

    async def _write_type_edges_async(
        self,
        session,
        edges,
        metrics: Optional[IndexingMetrics] = None,
        progress_callback: Optional[Callable[[int], None]] = None,
    ):
        grouped: Dict[tuple[str, str, str], list[Dict]] = {}
        for edge in edges:
            grouped.setdefault(
                (edge.relation, edge.child_label, edge.parent_label), []
            ).append(
                {
                    "child_name": edge.child_name,
                    "child_path": edge.child_path,
                    "child_line_number": edge.child_line_number,
                    "parent_name": edge.parent_name,
                    "parent_path": edge.parent_path,
                    "parent_line_number": edge.parent_line_number,
                }
            )
        for (relation, child_label, parent_label), rows in grouped.items():
            query = f"""
                UNWIND $rows AS row
                MATCH (child:{child_label} {{name: row.child_name, path: row.child_path, line_number: row.child_line_number}})
                MATCH (parent:{parent_label} {{name: row.parent_name, path: row.parent_path, line_number: row.parent_line_number}})
                MERGE (child)-[:{relation}]->(parent)
            """
            await self._run_batched_query_async(
                session,
                query,
                rows,
                fallback=lambda current_session, row, edge_relation=relation, edge_child=child_label, edge_parent=parent_label: current_session.run(
                    f"""
                    MATCH (child:{edge_child} {{name: $child_name, path: $child_path, line_number: $child_line_number}})
                    MATCH (parent:{edge_parent} {{name: $parent_name, path: $parent_path, line_number: $parent_line_number}})
                    MERGE (child)-[:{edge_relation}]->(parent)
                    """,
                    **row,
                ),
                progress_callback=progress_callback,
            )
        if metrics is not None:
            metrics.increment("created_type_edges", len(edges))

    def _create_function_calls(
        self,
        session,
        file_data: Dict,
        imports_map: dict,
        symbol_index: Optional[SymbolIndex] = None,
        metrics: Optional[IndexingMetrics] = None,
        allow_global_fallback: Optional[bool] = None,
    ):
        """Create CALLS relationships for a single file using in-memory resolution."""
        effective_symbol_index = symbol_index or SymbolIndex.build([file_data])
        effective_fallback = (
            self._global_fallback_enabled()
            if allow_global_fallback is None
            else allow_global_fallback
        )
        edges = collect_call_edges(
            [file_data],
            imports_map,
            effective_symbol_index,
            effective_fallback,
            metrics,
        )
        self._write_call_edges(session, edges, metrics)
        return edges

    def _create_all_function_calls(
        self,
        all_file_data: list[Dict],
        imports_map: dict,
        symbol_index: Optional[SymbolIndex] = None,
        metrics: Optional[IndexingMetrics] = None,
        allow_global_fallback: Optional[bool] = None,
    ):
        """Create CALLS relationships for all functions after all files have been processed."""
        debug_log(f"_create_all_function_calls called with {len(all_file_data)} files")
        effective_symbol_index = symbol_index or SymbolIndex.build(all_file_data)
        effective_fallback = (
            self._global_fallback_enabled()
            if allow_global_fallback is None
            else allow_global_fallback
        )
        edges = collect_call_edges(
            all_file_data,
            imports_map,
            effective_symbol_index,
            effective_fallback,
            metrics,
        )
        with self.driver.session() as session:
            self._write_call_edges(session, edges, metrics)

    def _create_inheritance_links(
        self,
        session,
        file_data: Dict,
        imports_map: dict,
        symbol_index: Optional[SymbolIndex] = None,
        metrics: Optional[IndexingMetrics] = None,
        allow_global_fallback: Optional[bool] = None,
    ):
        """Create INHERITS and IMPLEMENTS relationships for a single file using in-memory resolution."""
        effective_symbol_index = symbol_index or SymbolIndex.build([file_data])
        effective_fallback = (
            self._global_fallback_enabled()
            if allow_global_fallback is None
            else allow_global_fallback
        )
        edges = collect_inheritance_edges(
            [file_data],
            imports_map,
            effective_symbol_index,
            effective_fallback,
            metrics,
        )
        self._write_type_edges(session, edges, metrics)
        return edges

    def _create_all_inheritance_links(
        self,
        all_file_data: list[Dict],
        imports_map: dict,
        symbol_index: Optional[SymbolIndex] = None,
        metrics: Optional[IndexingMetrics] = None,
        allow_global_fallback: Optional[bool] = None,
    ):
        """Create INHERITS relationships for all classes after all files have been processed."""
        effective_symbol_index = symbol_index or SymbolIndex.build(all_file_data)
        effective_fallback = (
            self._global_fallback_enabled()
            if allow_global_fallback is None
            else allow_global_fallback
        )
        edges = collect_inheritance_edges(
            all_file_data,
            imports_map,
            effective_symbol_index,
            effective_fallback,
            metrics,
        )
        with self.driver.session() as session:
            self._write_type_edges(session, edges, metrics)

    def delete_file_from_graph(self, path: str):
        """Deletes a file and all its contained elements and relationships."""
        file_path_str = str(Path(path).resolve())
        with self.driver.session() as session:
            parents_res = session.run(
                """
                MATCH (f:File {path: $path})<-[:CONTAINS*]-(d:Directory)
                RETURN d.path as path ORDER BY d.path DESC
            """,
                path=file_path_str,
            )
            parent_paths = [record["path"] for record in parents_res]

            session.run(
                """
                MATCH (f:File {path: $path})
                OPTIONAL MATCH (f)-[:CONTAINS]->(element)
                DETACH DELETE f, element
                """,
                path=file_path_str,
            )
            info_logger(f"Deleted file and its elements from graph: {file_path_str}")

            for path in parent_paths:
                session.run(
                    """
                    MATCH (d:Directory {path: $path})
                    WHERE NOT (d)-[:CONTAINS]->()
                    DETACH DELETE d
                """,
                    path=path,
                )

    def delete_repository_from_graph(self, repo_path: str) -> bool:
        """Deletes a repository and all its contents from the graph. Returns True if deleted, False if not found."""
        repo_path_str = str(Path(repo_path).resolve())
        with self.driver.session() as session:
            # Check if it exists
            result = session.run(
                "MATCH (r:Repository {path: $path}) RETURN count(r) as cnt",
                path=repo_path_str,
            ).single()
            repo_exists = bool(result and result["cnt"] > 0)

            if repo_exists:
                session.run(
                    """MATCH (r:Repository {path: $path})
                              OPTIONAL MATCH (r)-[:CONTAINS*]->(e)
                              DETACH DELETE r, e""",
                    path=repo_path_str,
                )
                info_logger(
                    f"Deleted repository and its contents from graph: {repo_path_str}"
                )
                return True

            return False

    def update_file_in_graph(self, path: Path, repo_path: Path, imports_map: dict):
        """Updates a single file's nodes in the graph."""
        file_path_str = str(path.resolve())
        repo_name = repo_path.name

        self.delete_file_from_graph(file_path_str)

        if path.exists():
            file_data = self.parse_file(repo_path, path)

            if "error" not in file_data:
                self.add_file_to_graph(file_data, repo_name, imports_map)
                return file_data
            else:
                error_logger(
                    f"Skipping graph add for {file_path_str} due to parsing error: {file_data['error']}"
                )
                return None
        else:
            return {"deleted": True, "path": file_path_str}

    def parse_file(
        self, repo_path: Path, path: Path, is_dependency: bool = False
    ) -> Dict:
        """Parses a file with the appropriate language parser and extracts code elements."""
        parser = self.parsers.get(path.suffix)
        if not parser:
            warning_logger(
                f"No parser found for file extension {path.suffix}. Skipping {path}"
            )
            return {"path": str(path), "error": f"No parser for {path.suffix}"}

        debug_log(
            f"[parse_file] Starting parsing for: {path} with {parser.language_name} parser"
        )
        try:
            index_source = (
                get_config_value("INDEX_SOURCE") or "false"
            ).lower() == "true"
            if self._fast_mode_enabled():
                index_source = False
            if parser.language_name == "python":
                is_notebook = path.suffix == ".ipynb"
                file_data = parser.parse(
                    path,
                    is_dependency,
                    is_notebook=is_notebook,
                    index_source=index_source,
                )
            else:
                file_data = parser.parse(path, is_dependency, index_source=index_source)
            file_data["repo_path"] = str(repo_path)
            return file_data
        except Exception as e:
            error_logger(
                f"Error parsing {path} with {parser.language_name} parser: {e}"
            )
            debug_log(f"[parse_file] Error parsing {path}: {e}")
            return {"path": str(path), "error": str(e)}

    def estimate_processing_time(self, path: Path) -> Optional[Tuple[int, float]]:
        """Estimate processing time and file count"""
        try:
            supported_extensions = self.parsers.keys()
            if path.is_file():
                if path.suffix in supported_extensions:
                    files = [path]
                else:
                    return 0, 0.0  # Not a supported file type
            else:
                all_files = path.rglob("*")
                files = [
                    f
                    for f in all_files
                    if f.is_file() and f.suffix in supported_extensions
                ]

                # Filter default ignored directories
                ignore_dirs_str = get_config_value("IGNORE_DIRS") or ""
                if ignore_dirs_str:
                    ignore_dirs = {
                        d.strip().lower()
                        for d in ignore_dirs_str.split(",")
                        if d.strip()
                    }
                    if ignore_dirs:
                        kept_files = []
                        for f in files:
                            try:
                                parts = set(
                                    p.lower() for p in f.relative_to(path).parent.parts
                                )
                                if not parts.intersection(ignore_dirs):
                                    kept_files.append(f)
                            except ValueError:
                                kept_files.append(f)
                        files = kept_files

            total_files = len(files)
            estimated_time = total_files * 0.05  # tree-sitter is faster
            return total_files, estimated_time
        except Exception as e:
            error_logger(f"Could not estimate processing time for {path}: {e}")
            return None

    async def _build_graph_from_scip(
        self, path: Path, is_dependency: bool, job_id: Optional[str], lang: str
    ):
        """
        SCIP-based indexing path. Activated only when SCIP_INDEXER=true and
        a scip-<lang> binary is available.

        Steps:
          1. Run scip-<lang> CLI → index.scip
          2. Parse index.scip → nodes + reference edges
          3. Write nodes to graph (same MERGE queries as Tree-sitter path)
          4. Tree-sitter supplement: add source text + cyclomatic_complexity
          5. Write SCIP CALLS edges (precise, no heuristics)
        """
        import tempfile
        from .scip_indexer import ScipIndexer, ScipIndexParser
        from .graph_builder import TreeSitterParser  # supplement pass

        if job_id:
            self.job_manager.update_job(job_id, status=JobStatus.RUNNING)

        self.add_repository_to_graph(path, is_dependency)
        repo_name = path.name

        try:
            # Step 1: Run SCIP indexer
            with tempfile.TemporaryDirectory(prefix="cgc_scip_") as tmpdir:
                scip_file = ScipIndexer().run(path, lang, Path(tmpdir))

                if not scip_file:
                    warning_logger(
                        f"SCIP indexer produced no output for {path}. "
                        "Falling back to Tree-sitter."
                    )
                    # Hand off to Tree-sitter pipeline by re-calling without SCIP flag
                    # (the flag is checked at the start; override is not needed because
                    # we return here — caller will not re-enter this branch)
                    raise RuntimeError(
                        "SCIP produced no index — triggering Tree-sitter fallback"
                    )

                # Step 2: Parse index.scip
                scip_data = ScipIndexParser().parse(scip_file, path)

            if not scip_data:
                raise RuntimeError("SCIP parse returned empty result")

            files_data = scip_data.get("files", {})
            file_paths = [Path(p) for p in files_data.keys() if Path(p).exists()]

            # Step 3: Pre-scan for imports to correctly associate external modules/classes
            imports_map = self._pre_scan_for_imports(file_paths)

            if job_id:
                self.job_manager.update_job(job_id, total_files=len(files_data))

            # Step 4: Write nodes to graph using existing add_file_to_graph()
            processed = 0
            for abs_path_str, file_data in files_data.items():
                file_data["repo_path"] = str(path.resolve())
                if job_id:
                    self.job_manager.update_job(job_id, current_file=abs_path_str)

                # Step 5: Tree-sitter supplement — add source text, complexity, imports and bases
                file_path = Path(abs_path_str)
                if file_path.exists() and file_path.suffix in self.parsers:
                    try:
                        ts_parser = self.parsers[file_path.suffix]
                        ts_data = ts_parser.parse(
                            file_path, is_dependency, index_source=True
                        )
                        if "error" not in ts_data:
                            # 1. Functions: complexity, source, decorators
                            ts_funcs = {
                                f["name"]: f for f in ts_data.get("functions", [])
                            }
                            for f in file_data.get("functions", []):
                                ts_f = ts_funcs.get(f["name"])
                                if ts_f:
                                    f.update(
                                        {
                                            "source": ts_f.get("source"),
                                            "cyclomatic_complexity": ts_f.get(
                                                "cyclomatic_complexity", 1
                                            ),
                                            "decorators": ts_f.get("decorators", []),
                                        }
                                    )

                            # 2. Classes: bases (inheritance)
                            ts_classes = {
                                c["name"]: c for c in ts_data.get("classes", [])
                            }
                            for c in file_data.get("classes", []):
                                ts_c = ts_classes.get(c["name"])
                                if ts_c:
                                    c["bases"] = ts_c.get("bases", [])

                            # 3. Imports: critical for cross-file resolution
                            file_data["imports"] = ts_data.get("imports", [])

                            # 4. Variables/Other: value, etc.
                            file_data["variables"] = ts_data.get("variables", [])
                    except Exception as e:
                        debug_log(
                            f"Tree-sitter supplement failed for {abs_path_str}: {e}"
                        )

                self.add_file_to_graph(file_data, repo_name, imports_map)

                processed += 1
                if job_id:
                    self.job_manager.update_job(job_id, processed_files=processed)
                await asyncio.sleep(0.01)

            # Step 6: Create INHERITS relationships (Supplemented from Tree-sitter)
            self._create_all_inheritance_links(list(files_data.values()), imports_map)

            # Step 7: Write SCIP CALLS edges — precise cross-file resolution
            with self.driver.session() as session:
                for file_data in files_data.values():
                    for edge in file_data.get("function_calls_scip", []):
                        try:
                            # Use line numbers for precise matching in case of duplicates
                            session.run(
                                """
                                MATCH (caller:Function {name: $caller_name, path: $caller_file, line_number: $caller_line})
                                MATCH (callee:Function {name: $callee_name, path: $callee_file, line_number: $callee_line})
                                MERGE (caller)-[:CALLS {line_number: $ref_line, source: 'scip'}]->(callee)
                            """,
                                caller_name=self._name_from_symbol(
                                    edge["caller_symbol"]
                                ),
                                caller_file=edge["caller_file"],
                                caller_line=edge["caller_line"],
                                callee_name=edge["callee_name"],
                                callee_file=edge["callee_file"],
                                callee_line=edge["callee_line"],
                                ref_line=edge["ref_line"],
                            )
                        except Exception:
                            pass  # best-effort: node might not be indexed yet

            if job_id:
                self.job_manager.update_job(
                    job_id, status=JobStatus.COMPLETED, end_time=datetime.now()
                )

        except RuntimeError as e:
            # Graceful fallback to Tree-sitter when SCIP fails
            warning_logger(f"SCIP path failed ({e}), re-running with Tree-sitter...")
            # Temporarily disable the flag in-memory so the recursive call goes straight to TS
            # (we do this by calling the internal Tree-sitter steps directly)
            if job_id:
                self.job_manager.update_job(job_id, status=JobStatus.RUNNING)
            # Re-enter the async flow without SCIP check — handled by caller returning early
            # For simplicity, we just let the exception propagate to the outer handler so the
            # job is marked FAILED with a meaningful message rather than silently degrading.
            raise

        except Exception as e:
            error_logger(f"SCIP indexing failed for {path}: {e}")
            if job_id:
                self.job_manager.update_job(
                    job_id,
                    status=JobStatus.FAILED,
                    end_time=datetime.now(),
                    errors=[str(e)],
                    error_type=type(e).__name__,
                    error_details=traceback.format_exc(),
                )
            raise

    def _name_from_symbol(self, symbol: str) -> str:
        """Extract human-readable name from a SCIP symbol ID string."""
        import re

        s = symbol.rstrip(".#")
        s = re.sub(r"\(\)\.?$", "", s)  # Remove trailing () or ().
        parts = re.split(r"[/#]", s)
        last = parts[-1] if parts else symbol
        return last or symbol

    async def build_graph_from_path_async(
        self, path: Path, is_dependency: bool = False, job_id: str = None
    ):
        """Builds graph from a directory or file path."""
        metrics = IndexingMetrics()
        build_start = perf_counter()
        self._reset_query_timings()
        self._reset_row_size_cache()
        try:
            # ------------------------------------------------------------------
            # SCIP feature flag: SCIP_INDEXER=true in ~/.codegraphcontext/.env
            # When enabled (and the binary is installed), SCIP handles the
            # indexing for supported languages. SCIP_INDEXER=false (default)
            # means this entire block is a no-op and existing behaviour is kept.
            # ------------------------------------------------------------------
            scip_enabled = (
                get_config_value("SCIP_INDEXER") or "false"
            ).lower() == "true"
            if scip_enabled:
                from .scip_indexer import (
                    ScipIndexer,
                    ScipIndexParser,
                    detect_project_lang,
                    is_scip_available,
                )

                scip_langs_str = (
                    get_config_value("SCIP_LANGUAGES")
                    or "python,typescript,go,rust,java"
                )
                scip_languages = [
                    l.strip() for l in scip_langs_str.split(",") if l.strip()
                ]
                detected_lang = detect_project_lang(path, scip_languages)

                if detected_lang and is_scip_available(detected_lang):
                    info_logger(
                        f"SCIP_INDEXER=true — using SCIP for language: {detected_lang}"
                    )
                    await self._build_graph_from_scip(
                        path, is_dependency, job_id, detected_lang
                    )
                    return  # SCIP handled it; skip Tree-sitter pipeline below
                else:
                    if detected_lang:
                        warning_logger(
                            f"SCIP_INDEXER=true but scip-{detected_lang} binary not found. "
                            f"Falling back to Tree-sitter. Install it first."
                        )
                    else:
                        info_logger(
                            "SCIP_INDEXER=true but no SCIP-supported language detected. "
                            "Falling back to Tree-sitter."
                        )
            # ------------------------------------------------------------------
            # Existing Tree-sitter pipeline (unchanged)
            # ------------------------------------------------------------------
            if job_id:
                self.job_manager.update_job(
                    job_id, status=JobStatus.RUNNING, current_phase="discover"
                )

            self.add_repository_to_graph(path, is_dependency)
            repo_name = path.name

            # Search for .cgcignore upwards
            cgcignore_path = None
            ignore_root = path.resolve()

            # Start search from path (or parent if path is file)
            curr = path.resolve()
            if not curr.is_dir():
                curr = curr.parent

            # Walk up looking for .cgcignore
            while True:
                candidate = curr / ".cgcignore"
                if candidate.exists():
                    cgcignore_path = candidate
                    ignore_root = curr
                    debug_log(f"Found .cgcignore at {ignore_root}")
                    break
                if curr.parent == curr:  # Root hit
                    break
                curr = curr.parent

            if cgcignore_path:
                with open(cgcignore_path) as f:
                    ignore_patterns = f.read().splitlines()
                spec = pathspec.PathSpec.from_lines("gitwildmatch", ignore_patterns)
            else:
                spec = None

            supported_extensions = self.parsers.keys()
            all_files = path.rglob("*") if path.is_dir() else [path]
            files = [
                f for f in all_files if f.is_file() and f.suffix in supported_extensions
            ]

            # Filter default ignored directories
            ignore_dirs_str = get_config_value("IGNORE_DIRS") or ""
            if ignore_dirs_str and path.is_dir():
                ignore_dirs = {
                    d.strip().lower() for d in ignore_dirs_str.split(",") if d.strip()
                }
                if ignore_dirs:
                    kept_files = []
                    for f in files:
                        try:
                            # Check if any parent directory in the relative path is in ignore list
                            parts = set(
                                p.lower() for p in f.relative_to(path).parent.parts
                            )
                            if not parts.intersection(ignore_dirs):
                                kept_files.append(f)
                            else:
                                # debug_log(f"Skipping default ignored file: {f}")
                                pass
                        except ValueError:
                            kept_files.append(f)
                    files = kept_files

            if spec:
                filtered_files = []
                for f in files:
                    try:
                        # Match relative to the directory containing .cgcignore
                        rel_path = f.relative_to(ignore_root)
                        if not spec.match_file(str(rel_path)):
                            filtered_files.append(f)
                        else:
                            debug_log(f"Ignored file based on .cgcignore: {rel_path}")
                    except ValueError:
                        # Should not happen if ignore_root is a parent, but safety fallback
                        filtered_files.append(f)
                files = filtered_files

            files, oversized_files = self._filter_oversized_files(files)
            if oversized_files:
                metrics.increment("skipped_oversized_files", len(oversized_files))
                warning_logger(
                    f"Skipping {len(oversized_files)} file(s) larger than {self._get_max_index_file_bytes()} bytes"
                )

            if job_id:
                self.job_manager.update_job(job_id, total_files=len(files))

            allow_global_fallback = self._global_fallback_enabled()
            large_repo_threshold = self._get_large_repo_threshold()
            if len(files) >= large_repo_threshold and allow_global_fallback:
                allow_global_fallback = False
                metrics.increment("guardrail_disabled_global_fallback")
                warning_logger(
                    f"Disabling global fallback resolution for large repository ({len(files)} files >= {large_repo_threshold})."
                )

            if len(files) >= large_repo_threshold:
                extensions = {file.suffix for file in files}
                if any(ext in extensions for ext in {".go", ".java", ".ts", ".tsx"}):
                    warning_logger(
                        "Large repository detected on a language where SCIP is typically more scalable. "
                        "Consider enabling SCIP_INDEXER for better indexing throughput."
                    )

            self._update_job_phase(job_id, "pre_scan")
            phase_start = perf_counter()
            debug_log("Starting pre-scan to build imports map...")
            imports_map = self._pre_scan_for_imports(files)
            debug_log(f"Pre-scan complete. Found {len(imports_map)} definitions.")
            metrics.add_timing("pre_scan", perf_counter() - phase_start)

            all_file_data = []

            processed_count = 0
            self._reset_job_progress(job_id, "parse", len(files), progress_unit="files")
            phase_start = perf_counter()
            for file in files:
                if file.is_file():
                    if job_id:
                        self.job_manager.update_job(job_id, current_file=str(file))
                    await asyncio.sleep(0)
                    repo_path = (
                        path.resolve() if path.is_dir() else file.parent.resolve()
                    )
                    file_data = self.parse_file(repo_path, file, is_dependency)
                    if "error" not in file_data:
                        all_file_data.append(file_data)
                    processed_count += 1
                    if job_id:
                        self.job_manager.update_job(
                            job_id, processed_files=processed_count
                        )
                    await asyncio.sleep(0.01)
            metrics.add_timing("parse", perf_counter() - phase_start)

            self._update_job_phase(job_id, "prepare_write_nodes")
            phase_start = perf_counter()
            file_batch_rows, total_write_rows = self._prepare_file_batch_rows_with_stats(
                all_file_data
            )
            total_write_batches = 0
            metrics.add_timing("prepare_write_nodes", perf_counter() - phase_start)
            self._reset_job_progress(
                job_id,
                "write_nodes",
                total_write_rows,
                "files",
                progress_unit="entries",
                progress_detail=self._format_progress_detail(
                    "files",
                    0,
                    total_write_rows,
                    "entries",
                    0,
                    total_write_batches,
                ),
                total_batches=total_write_batches,
            )
            phase_start = perf_counter()
            written_rows = 0
            written_batches = 0
            last_write_report_at = perf_counter()

            def report_write_progress(label: str, count: int):
                nonlocal written_rows
                nonlocal written_batches
                nonlocal last_write_report_at
                written_rows += count
                if total_write_batches > 0:
                    written_batches += 1
                if job_id:
                    now = perf_counter()
                    display_label = (
                        f"{label} (fallback)"
                        if getattr(self, "_write_progress_mode", "batch") == "fallback"
                        else label
                    )
                    if (
                        written_rows == total_write_rows
                        or count >= 25
                        or written_rows % 25 == 0
                        or now - last_write_report_at >= 0.25
                    ):
                        self.job_manager.update_job(
                            job_id,
                            processed_files=written_rows,
                            current_file=display_label,
                            current_batch=written_batches,
                            progress_detail=self._format_progress_detail(
                                display_label,
                                written_rows,
                                total_write_rows,
                                "entries",
                                written_batches,
                                total_write_batches,
                            ),
                        )
                        last_write_report_at = now

            with self.driver.session() as session:
                await self._write_file_batch_rows_async(
                    session,
                    file_batch_rows,
                    metrics=metrics,
                    progress_callback=report_write_progress,
                )
            metrics.add_timing("write_nodes", perf_counter() - phase_start)

            symbol_index = SymbolIndex.build(all_file_data)

            self._reset_job_progress(
                job_id,
                "link_inheritance",
                1,
                "resolving inheritance",
                progress_unit="entries",
            )
            phase_start = perf_counter()
            inheritance_edges = collect_inheritance_edges(
                all_file_data,
                imports_map,
                symbol_index,
                allow_global_fallback,
                metrics,
            )
            grouped_inheritance_edges: Dict[tuple[str, str, str], list[Dict]] = {}
            for edge in inheritance_edges:
                grouped_inheritance_edges.setdefault(
                    (edge.relation, edge.child_label, edge.parent_label), []
                ).append(
                    {
                        "child_name": edge.child_name,
                        "child_path": edge.child_path,
                        "child_line_number": edge.child_line_number,
                        "parent_name": edge.parent_name,
                        "parent_path": edge.parent_path,
                        "parent_line_number": edge.parent_line_number,
                    }
                )
            total_inheritance_edges = len(inheritance_edges)
            total_inheritance_batches = 0
            self._reset_job_progress(
                job_id,
                "link_inheritance",
                total_inheritance_edges,
                "inheritance edges",
                progress_unit="entries",
                progress_detail=self._format_progress_detail(
                    "inheritance edges",
                    0,
                    total_inheritance_edges,
                    "entries",
                    0,
                    total_inheritance_batches,
                ),
                total_batches=total_inheritance_batches,
            )
            written_inheritance_edges = 0
            written_inheritance_batches = 0
            last_inheritance_report_at = perf_counter()

            def report_inheritance_progress(count: int):
                nonlocal written_inheritance_edges
                nonlocal written_inheritance_batches
                nonlocal last_inheritance_report_at
                written_inheritance_edges += count
                if total_inheritance_batches > 0:
                    written_inheritance_batches += 1
                if job_id:
                    now = perf_counter()
                    if (
                        written_inheritance_edges == total_inheritance_edges
                        or count >= 25
                        or written_inheritance_edges % 25 == 0
                        or now - last_inheritance_report_at >= 0.25
                    ):
                        self.job_manager.update_job(
                            job_id,
                            processed_files=written_inheritance_edges,
                            current_file="inheritance edges",
                            current_batch=written_inheritance_batches,
                            progress_detail=self._format_progress_detail(
                                "inheritance edges",
                                written_inheritance_edges,
                                total_inheritance_edges,
                                "entries",
                                written_inheritance_batches,
                                total_inheritance_batches,
                            ),
                        )
                        last_inheritance_report_at = now

            with self.driver.session() as session:
                await self._write_type_edges_async(
                    session,
                    inheritance_edges,
                    metrics=metrics,
                    progress_callback=report_inheritance_progress,
                )
            metrics.add_timing("link_inheritance", perf_counter() - phase_start)

            self._reset_job_progress(
                job_id, "link_calls", 1, "resolving call edges", progress_unit="entries"
            )
            phase_start = perf_counter()
            call_edges = collect_call_edges(
                all_file_data,
                imports_map,
                symbol_index,
                allow_global_fallback,
                metrics,
            )
            grouped_call_edges: Dict[tuple[str, str], list[Dict]] = {}
            for edge in call_edges:
                grouped_call_edges.setdefault(
                    (edge.caller_label, edge.callee_label), []
                ).append(
                    {
                        "caller_name": edge.caller_name,
                        "caller_path": edge.caller_path,
                        "caller_line_number": edge.caller_line_number,
                        "callee_name": edge.callee_name,
                        "callee_path": edge.callee_path,
                        "callee_line_number": edge.callee_line_number,
                        "line_number": edge.line_number,
                        "args": edge.args,
                        "full_call_name": edge.full_call_name,
                    }
                )
            total_call_edges = len(call_edges)
            total_call_batches = 0
            self._reset_job_progress(
                job_id,
                "link_calls",
                total_call_edges,
                "call edges",
                progress_unit="entries",
                progress_detail=self._format_progress_detail(
                    "call edges",
                    0,
                    total_call_edges,
                    "entries",
                    0,
                    total_call_batches,
                ),
                total_batches=total_call_batches,
            )
            written_call_edges = 0
            written_call_batches = 0
            last_call_report_at = perf_counter()

            def report_call_progress(count: int):
                nonlocal written_call_edges
                nonlocal written_call_batches
                nonlocal last_call_report_at
                written_call_edges += count
                if total_call_batches > 0:
                    written_call_batches += 1
                if job_id:
                    now = perf_counter()
                    if (
                        written_call_edges == total_call_edges
                        or count >= 25
                        or written_call_edges % 25 == 0
                        or now - last_call_report_at >= 0.25
                    ):
                        self.job_manager.update_job(
                            job_id,
                            processed_files=written_call_edges,
                            current_file="call edges",
                            current_batch=written_call_batches,
                            progress_detail=self._format_progress_detail(
                                "call edges",
                                written_call_edges,
                                total_call_edges,
                                "entries",
                                written_call_batches,
                                total_call_batches,
                            ),
                        )
                        last_call_report_at = now

            with self.driver.session() as session:
                await self._write_call_edges_async(
                    session,
                    call_edges,
                    metrics=metrics,
                    progress_callback=report_call_progress,
                )
            metrics.add_timing("link_calls", perf_counter() - phase_start)

            summary = metrics.summary()
            summary["query_timings"] = self._query_timing_summary()
            info_logger(f"Indexing summary for {path}: {summary}")

            if job_id:
                self.job_manager.update_job(
                    job_id,
                    status=JobStatus.COMPLETED,
                    current_phase="completed",
                    end_time=datetime.now(),
                    actual_duration=round(perf_counter() - build_start, 4),
                    result=summary,
                )
        except Exception as e:
            error_message = str(e)
            error_logger(f"Failed to build graph for path {path}: {error_message}")
            error_details = traceback.format_exc()
            error_logger(error_details)
            if job_id:
                """checking if the repo got deleted"""
                if (
                    "no such file found" in error_message
                    or "deleted" in error_message
                    or "not found" in error_message
                ):
                    status = JobStatus.CANCELLED

                else:
                    status = JobStatus.FAILED

                self.job_manager.update_job(
                    job_id,
                    status=status,
                    current_phase="failed",
                    end_time=datetime.now(),
                    actual_duration=round(perf_counter() - build_start, 4),
                    errors=[str(e)],
                    error_type=type(e).__name__,
                    error_details=error_details,
                    result=metrics.summary(),
                )
            raise
