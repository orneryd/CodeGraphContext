import asyncio
import json

from codegraphcontext.tools.indexing_scalability import (
    IndexingMetrics,
    SymbolIndex,
    collect_call_edges,
    collect_inheritance_edges,
)
from codegraphcontext.tools.graph_builder import GraphBuilder


def _file_data(path, *, functions=None, classes=None, calls=None, imports=None):
    return {
        "path": path,
        "functions": functions or [],
        "classes": classes or [],
        "traits": [],
        "interfaces": [],
        "structs": [],
        "records": [],
        "imports": imports or [],
        "function_calls": calls or [],
    }


def test_collect_call_edges_uses_local_resolution_without_fanout():
    file_data = _file_data(
        "/repo/a.py",
        functions=[
            {"name": "foo", "line_number": 1, "class_context": None},
            {"name": "caller", "line_number": 10, "class_context": None},
        ],
        calls=[
            {
                "name": "foo",
                "full_name": "foo",
                "line_number": 12,
                "args": [],
                "context": ("caller", "function_definition", 10),
                "class_context": None,
            }
        ],
    )
    other = _file_data(
        "/repo/b.py",
        functions=[{"name": "foo", "line_number": 2, "class_context": None}],
    )

    metrics = IndexingMetrics()
    edges = collect_call_edges(
        [file_data, other], {}, SymbolIndex.build([file_data, other]), False, metrics
    )

    assert len(edges) == 1
    assert edges[0].callee_path == "/repo/a.py"
    assert metrics.counters["resolved_calls"] == 1


def test_collect_call_edges_leaves_ambiguous_global_name_unresolved_by_default():
    caller = _file_data(
        "/repo/caller.py",
        functions=[{"name": "caller", "line_number": 10, "class_context": None}],
        calls=[
            {
                "name": "shared",
                "full_name": "shared",
                "line_number": 12,
                "args": [],
                "context": ("caller", "function_definition", 10),
                "class_context": None,
            }
        ],
    )
    left = _file_data(
        "/repo/left.py",
        functions=[{"name": "shared", "line_number": 1, "class_context": None}],
    )
    right = _file_data(
        "/repo/right.py",
        functions=[{"name": "shared", "line_number": 1, "class_context": None}],
    )

    metrics = IndexingMetrics()
    edges = collect_call_edges(
        [caller, left, right],
        {},
        SymbolIndex.build([caller, left, right]),
        False,
        metrics,
    )

    assert edges == []
    assert metrics.counters["unresolved_calls"] == 1


def test_collect_call_edges_resolves_import_qualified_target():
    caller = _file_data(
        "/repo/caller.py",
        functions=[{"name": "caller", "line_number": 10, "class_context": None}],
        calls=[
            {
                "name": "shared",
                "full_name": "dep.shared",
                "line_number": 12,
                "args": [],
                "context": ("caller", "function_definition", 10),
                "class_context": None,
            }
        ],
        imports=[{"name": "pkg.dep", "alias": "dep"}],
    )
    callee = _file_data(
        "/repo/pkg/dep.py",
        functions=[{"name": "shared", "line_number": 1, "class_context": None}],
    )
    imports_map = {"pkg.dep": ["/repo/pkg/dep.py"], "dep": ["/repo/pkg/dep.py"]}

    edges = collect_call_edges(
        [caller, callee],
        imports_map,
        SymbolIndex.build([caller, callee]),
        False,
        IndexingMetrics(),
    )

    assert len(edges) == 1
    assert edges[0].callee_path == "/repo/pkg/dep.py"


def test_collect_call_edges_normalizes_structured_call_args_to_strings():
    caller = _file_data(
        "/repo/caller.cpp",
        functions=[{"name": "caller", "line_number": 10, "class_context": None}],
        calls=[
            {
                "name": "target",
                "full_name": "target",
                "line_number": 12,
                "args": [{"name": "value", "type": "int"}, ["nested", 2], True],
                "context": ("caller", "function_definition", 10),
                "class_context": None,
            }
        ],
    )
    callee = _file_data(
        "/repo/target.cpp",
        functions=[{"name": "target", "line_number": 1, "class_context": None}],
    )

    edges = collect_call_edges(
        [caller, callee],
        {},
        SymbolIndex.build([caller, callee]),
        True,
        IndexingMetrics(),
    )

    assert len(edges) == 1
    assert edges[0].args == [
        '{"name": "value", "type": "int"}',
        '["nested", 2]',
        "True",
    ]


def test_prepare_file_batch_rows_normalizes_function_args_for_supported_fallback_write_path():
    builder = GraphBuilder.__new__(GraphBuilder)

    rows = builder._prepare_file_batch_rows(
        [
            {
                "path": "/repo/caller.cpp",
                "repo_path": "/repo",
                "functions": [
                    {
                        "name": "caller",
                        "line_number": 10,
                        "args": [
                            {"name": "value", "type": "int"},
                            ["nested", 2],
                            True,
                        ],
                    }
                ],
                "classes": [],
                "traits": [],
                "variables": [],
                "interfaces": [],
                "macros": [],
                "structs": [],
                "enums": [],
                "unions": [],
                "records": [],
                "properties": [],
                "modules": [],
                "imports": [],
                "module_inclusions": [],
                "is_dependency": False,
                "lang": "cpp",
            }
        ]
    )

    function_row = rows["nodes"]["Function"][0]

    assert function_row["props"]["args"] == [
        '{"name": "value", "type": "int"}',
        '["nested", 2]',
        "True",
    ]
    assert rows["parameters"] == [
        {
            "func_name": "caller",
            "path": "/repo/caller.cpp",
            "function_line_number": 10,
            "name": "value",
        },
        {
            "func_name": "caller",
            "path": "/repo/caller.cpp",
            "function_line_number": 10,
            "name": '["nested", 2]',
        },
        {
            "func_name": "caller",
            "path": "/repo/caller.cpp",
            "function_line_number": 10,
            "name": "True",
        },
    ]


def test_get_batch_target_bytes_is_hard_capped_to_128kb():
    builder = GraphBuilder.__new__(GraphBuilder)

    assert builder._get_batch_target_bytes() == 131072


def test_filter_oversized_files_skips_files_over_32kb(tmp_path):
    builder = GraphBuilder.__new__(GraphBuilder)

    small_file = tmp_path / "small.py"
    small_file.write_bytes(b"a" * 1024)
    large_file = tmp_path / "large.py"
    large_file.write_bytes(b"b" * 32769)

    kept_files, skipped_files = builder._filter_oversized_files([small_file, large_file])

    assert kept_files == [small_file]
    assert skipped_files == [large_file]


def test_prepare_file_batch_rows_deduplicates_directory_and_contains_rows():
    builder = GraphBuilder.__new__(GraphBuilder)

    rows = builder._prepare_file_batch_rows(
        [
            {
                "path": "/repo/tests/a.py",
                "repo_path": "/repo",
                "functions": [],
                "classes": [],
                "traits": [],
                "variables": [],
                "interfaces": [],
                "macros": [],
                "structs": [],
                "enums": [],
                "unions": [],
                "records": [],
                "properties": [],
                "modules": [],
                "imports": [],
                "module_inclusions": [],
                "is_dependency": False,
                "lang": "python",
            },
            {
                "path": "/repo/tests/b.py",
                "repo_path": "/repo",
                "functions": [],
                "classes": [],
                "traits": [],
                "variables": [],
                "interfaces": [],
                "macros": [],
                "structs": [],
                "enums": [],
                "unions": [],
                "records": [],
                "properties": [],
                "modules": [],
                "imports": [],
                "module_inclusions": [],
                "is_dependency": False,
                "lang": "python",
            },
        ]
    )

    assert rows["directories"] == [{"path": "/repo/tests", "name": "tests"}]
    assert rows["repo_to_dir"] == [{"repo_path": "/repo", "dir_path": "/repo/tests"}]
    assert rows["dir_to_file"] == [
        {"dir_path": "/repo/tests", "file_path": "/repo/tests/a.py"},
        {"dir_path": "/repo/tests", "file_path": "/repo/tests/b.py"},
    ]


def test_prepare_file_batch_rows_with_stats_tracks_total_rows_without_extra_count_pass():
    builder = GraphBuilder.__new__(GraphBuilder)

    rows, total_rows = builder._prepare_file_batch_rows_with_stats(
        [
            {
                "path": "/repo/tests/a.py",
                "repo_path": "/repo",
                "functions": [],
                "classes": [],
                "traits": [],
                "variables": [],
                "interfaces": [],
                "macros": [],
                "structs": [],
                "enums": [],
                "unions": [],
                "records": [],
                "properties": [],
                "modules": [],
                "imports": [],
                "module_inclusions": [],
                "is_dependency": False,
                "lang": "python",
            },
            {
                "path": "/repo/tests/b.py",
                "repo_path": "/repo",
                "functions": [],
                "classes": [],
                "traits": [],
                "variables": [],
                "interfaces": [],
                "macros": [],
                "structs": [],
                "enums": [],
                "unions": [],
                "records": [],
                "properties": [],
                "modules": [],
                "imports": [],
                "module_inclusions": [],
                "is_dependency": False,
                "lang": "python",
            },
        ]
    )

    assert total_rows == 6
    assert total_rows == (
        len(rows["files"])
        + len(rows["directories"])
        + len(rows["repo_to_dir"])
        + len(rows["dir_to_dir"])
        + len(rows["repo_to_file"])
        + len(rows["dir_to_file"])
        + len(rows["parameters"])
        + len(rows["modules"])
        + len(rows["imports"])
        + len(rows["nested_function_contains"])
        + len(rows["class_contains"])
        + len(rows["module_inclusions"])
        + sum(len(label_rows) for label_rows in rows["nodes"].values())
    )


def test_build_batched_node_write_query_aliases_row_fields_before_merge():
    builder = GraphBuilder.__new__(GraphBuilder)

    query = builder._build_batched_node_write_query("Variable")

    assert "WITH row.file_path AS file_path, row.name AS name, row.line_number AS line_number, row.props AS props" in query
    assert "MERGE (n:Variable {name: name, path: file_path, line_number: line_number})" in query
    assert "MERGE (n:Variable {name: row.name, path: row.file_path, line_number: row.line_number})" not in query


def test_collect_inheritance_edges_avoids_global_name_guessing():
    child = _file_data(
        "/repo/child.py",
        classes=[{"name": "Child", "line_number": 1, "bases": ["Parent"]}],
    )
    left_parent = _file_data(
        "/repo/left.py", classes=[{"name": "Parent", "line_number": 1, "bases": []}]
    )
    right_parent = _file_data(
        "/repo/right.py", classes=[{"name": "Parent", "line_number": 1, "bases": []}]
    )

    metrics = IndexingMetrics()
    edges = collect_inheritance_edges(
        [child, left_parent, right_parent],
        {},
        SymbolIndex.build([child, left_parent, right_parent]),
        False,
        metrics,
    )

    assert edges == []
    assert metrics.counters["unresolved_inheritance"] == 1


def test_graph_builder_disables_unwind_batch_queries_for_portability():
    builder = GraphBuilder.__new__(GraphBuilder)

    assert builder._supports_unwind_batch_queries() is False


def test_run_batched_query_uses_fallback_when_unwind_batches_disabled():
    builder = GraphBuilder.__new__(GraphBuilder)
    builder._get_batch_target_bytes = lambda: 2
    builder._estimate_batch_row_size = lambda row: 1

    class Session:
        def __init__(self):
            self.run_calls = 0

        def run(self, query, **params):
            self.run_calls += 1
            raise AssertionError("UNWIND batch query should not be used")

    session = Session()
    seen = []
    progress = []

    def fallback(current_session, row):
        seen.append((current_session, row["path"]))

    rows = [
        {"path": "/repo/a.py"},
        {"path": "/repo/b.py"},
        {"path": "/repo/c.py"},
    ]

    builder._run_batched_query(
        session,
        "UNWIND $rows AS row RETURN row.path",
        rows,
        fallback=fallback,
        progress_callback=progress.append,
    )

    assert session.run_calls == 0
    assert [path for _, path in seen] == ["/repo/a.py", "/repo/b.py", "/repo/c.py"]
    assert progress == [1, 1, 1]


def test_run_batched_query_wraps_fallback_failures_with_query_and_row_context():
    builder = GraphBuilder.__new__(GraphBuilder)
    builder._get_batch_target_bytes = lambda: 10
    builder._estimate_batch_row_size = lambda row: 1

    class Session:
        def run(self, query, **params):
            raise AssertionError("UNWIND batch query should not be used")

    def fallback(current_session, row):
        raise ValueError("already exists")

    try:
        builder._run_batched_query(
            Session(),
            "MATCH (f:File {path: $file_path}) MERGE (n:Function {name: $name, path: $file_path, line_number: $line_number}) SET n += $props",
            [{"file_path": "/repo/a.py", "name": "dup", "line_number": 1}],
            fallback=fallback,
        )
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected wrapped RuntimeError")

    assert "Graph write failed: ValueError: already exists" in message
    assert "MERGE (n:Function" in message
    assert '"name": "dup"' in message


def test_iter_sized_batches_uses_payload_size():
    builder = GraphBuilder.__new__(GraphBuilder)
    builder._get_batch_target_bytes = lambda: 6
    builder._estimate_batch_row_size = lambda row: row["size"]

    rows = [
        {"path": "/repo/a.py", "size": 3},
        {"path": "/repo/b.py", "size": 3},
        {"path": "/repo/c.py", "size": 3},
    ]

    batches = list(builder._iter_sized_batches(rows))

    assert [[row["path"] for row in batch] for batch in batches] == [
        ["/repo/a.py", "/repo/b.py"],
        ["/repo/c.py"],
    ]


def test_count_file_batch_chunks_sums_all_categories():
    builder = GraphBuilder.__new__(GraphBuilder)
    builder._count_batched_chunks = lambda rows: 0 if not rows else 1

    rows = {
        "files": [{"path": "/repo/a.py"}],
        "directories": [{"path": "/repo/dir"}],
        "repo_to_dir": [],
        "dir_to_dir": [],
        "repo_to_file": [{"file_path": "/repo/a.py"}],
        "dir_to_file": [],
        "nodes": {
            "Function": [{"name": "fn1"}, {"name": "fn2"}],
            "Class": [{"name": "Cls"}],
        },
        "parameters": [{"name": "arg"}],
        "modules": [],
        "imports": [{"module_name": "os"}],
        "nested_function_contains": [],
        "class_contains": [],
        "module_inclusions": [],
    }

    assert builder._count_file_batch_chunks(rows) == 7


def test_run_batched_query_async_yields_progress_per_chunk():
    builder = GraphBuilder.__new__(GraphBuilder)
    builder._get_batch_target_bytes = lambda: 2
    builder._estimate_batch_row_size = lambda row: 1

    class Session:
        def __init__(self):
            self.run_calls = 0

        def run(self, query, **params):
            self.run_calls += 1
            raise AssertionError("UNWIND batch query should not be used")

    session = Session()
    seen = []
    progress = []

    def fallback(current_session, row):
        seen.append((current_session, row["path"]))

    rows = [
        {"path": "/repo/a.py"},
        {"path": "/repo/b.py"},
        {"path": "/repo/c.py"},
    ]

    asyncio.run(
        builder._run_batched_query_async(
            session,
            "UNWIND $rows AS row RETURN row.path",
            rows,
            fallback=fallback,
            progress_callback=progress.append,
        )
    )

    assert session.run_calls == 0
    assert [path for _, path in seen] == ["/repo/a.py", "/repo/b.py", "/repo/c.py"]
    assert progress == [1, 1, 1]


def test_supports_unwind_batch_queries_for_neo4j_backend():
    builder = GraphBuilder.__new__(GraphBuilder)

    class DbManager:
        def get_backend_type(self):
            return "neo4j"

    builder.db_manager = DbManager()

    assert builder._supports_unwind_batch_queries() is True


def test_supports_unwind_batch_queries_disabled_for_non_neo4j_backend():
    builder = GraphBuilder.__new__(GraphBuilder)

    class DbManager:
        def get_backend_type(self):
            return "kuzudb"

    builder.db_manager = DbManager()

    assert builder._supports_unwind_batch_queries() is False


def test_run_batched_query_raises_on_neo4j_batch_failure_without_fallback():
    builder = GraphBuilder.__new__(GraphBuilder)
    builder._get_batch_target_bytes = lambda: 10
    builder._estimate_batch_row_size = lambda row: 1

    class DbManager:
        def get_backend_type(self):
            return "neo4j"

    class Session:
        def run(self, query, **params):
            raise RuntimeError("batch boom")

    builder.db_manager = DbManager()
    fallback_calls = []

    def fallback(current_session, row):
        fallback_calls.append(row)

    try:
        builder._run_batched_query(
            Session(),
            "UNWIND $rows AS row RETURN row.path",
            [{"path": "/repo/a.py"}],
            fallback=fallback,
        )
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected batch failure to be raised")

    assert "Graph write failed: RuntimeError: batch boom" in message
    assert "Batch rows: 1" in message
    assert fallback_calls == []


def test_estimate_batch_row_size_uses_cache():
    builder = GraphBuilder.__new__(GraphBuilder)
    builder._row_size_cache = {}
    row = {"path": "/repo/a.py", "name": "a.py"}

    original_dumps = json.dumps
    calls = {"count": 0}

    def counting_dumps(*args, **kwargs):
        calls["count"] += 1
        return original_dumps(*args, **kwargs)

    json.dumps = counting_dumps
    try:
        first = builder._estimate_batch_row_size(row)
        second = builder._estimate_batch_row_size(row)
    finally:
        json.dumps = original_dumps

    assert first == second
    assert calls["count"] == 1


def test_count_file_batch_rows_sums_all_row_categories():
    builder = GraphBuilder.__new__(GraphBuilder)

    rows = {
        "files": [{"path": "/repo/a.py"}],
        "directories": [{"path": "/repo/dir"}],
        "repo_to_dir": [],
        "dir_to_dir": [],
        "repo_to_file": [{"file_path": "/repo/a.py"}],
        "dir_to_file": [],
        "nodes": {
            "Function": [{"name": "fn1"}, {"name": "fn2"}],
            "Class": [{"name": "Cls"}],
        },
        "parameters": [{"name": "arg"}],
        "modules": [],
        "imports": [{"module_name": "os"}],
        "nested_function_contains": [],
        "class_contains": [],
        "module_inclusions": [],
    }

    assert builder._count_file_batch_rows(rows) == 8
