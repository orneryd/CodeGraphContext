# Indexing Scalability Plan

## Goal

Make `cgc index` complete reliably on repositories with hundreds to low-thousands of files by removing graph-size-dependent query amplification, reducing database round trips, and making expensive resolution steps bounded and observable.

## Problem Summary

The current Tree-sitter indexing path in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L1171) scales poorly because it combines:

1. Per-file, per-symbol `session.run(...)` calls in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L299).
2. Full-repository second passes for inheritance and call linking in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L736) and [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L875).
3. Multi-step fallback resolution per call site in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L504).
4. Global fallback matches by bare symbol name, especially `OPTIONAL MATCH (called:Function {name: $called_name})`, in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L691) and [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L730).
5. `MERGE` patterns that are not backed by matching constraints or indexes, especially for `Parameter` nodes in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L386).

The result is that later files pay against a larger and larger graph. Throughput drops sharply once enough functions, parameters, and ambiguous names have accumulated.

## Desired End State

The indexer should have these properties:

1. Runtime grows approximately linearly with file count and total extracted symbols.
2. One call site should never cause unbounded global callee fan-out.
3. Expensive linking work should be bounded by in-memory symbol maps or targeted indexed lookups.
4. Database writes should be batched instead of issued as thousands of tiny queries.
5. The indexer should expose phase timings and counts so regressions are visible.

## Work Plan

## Phase 1: Stop the Worst Growth Behavior

### 1. Remove relaxed global call fallbacks by default

Change `_create_function_calls(...)` in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L504) so unresolved calls do not fall back to graph-wide `Function {name: ...}` searches.

Required changes:

1. Delete or gate the relaxed fallback queries in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L691) and [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L730).
2. Make path-qualified or module-qualified resolution mandatory for cross-file call linking in the Tree-sitter path.
3. Preserve unresolved calls as metadata rather than forcing a guessed `CALLS` edge.

Reasoning:

The current relaxed fallback is the main graph-size-dependent multiplier. It trades correctness for a large and growing amount of work.

Acceptance criteria:

1. A call site produces at most one resolved callee unless an explicit multi-target mode is enabled.
2. Large repositories no longer show late-run collapse caused by fallback fan-out.
3. Unresolved call counts are reported explicitly.

### 2. Make external resolution opt-in, not opt-out

The config key `SKIP_EXTERNAL_RESOLUTION` exists in [src/codegraphcontext/cli/config_manager.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/cli/config_manager.py#L17), but the current default still allows the expensive path.

Required changes:

1. Change the default for `SKIP_EXTERNAL_RESOLUTION` to `true`.
2. Rename it to a clearer positive flag such as `ENABLE_GLOBAL_FALLBACK_RESOLUTION` while keeping backward compatibility.
3. Surface the setting in CLI help and large-repo guidance.

Acceptance criteria:

1. Default indexing behavior is conservative and bounded.
2. Users must explicitly opt into expensive fallback resolution.

## Phase 2: Replace Per-Call Graph Resolution with In-Memory Resolution

### 3. Build a symbol table during pre-scan and parse passes

Extend the existing pre-scan flow in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L184) and [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L1275) to construct an in-memory symbol index keyed by:

1. `name`
2. `name + file path`
3. `module/import alias + name`
4. class-local method identity where available

Required changes:

1. Add a dedicated symbol-index builder fed by parsed file data.
2. Teach `_create_function_calls(...)` and inheritance linking to resolve against that in-memory index first.
3. Limit DB writes to relationship creation after a target has already been resolved in memory.

Reasoning:

The database should not be used as the primary resolver for symbol identity while the index is still being built.

Acceptance criteria:

1. The number of DB queries in call-linking becomes proportional to the number of created edges, not the number of attempted resolution strategies.
2. Most successful call links are resolved without database reads.

### 4. Resolve inheritance in memory as well

The inheritance pass in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L875) should use the same symbol index instead of fallback graph matches.

Required changes:

1. Share the same symbol-table abstraction between inheritance and call linking.
2. Remove broad parent lookups such as `MATCH (parent {name: $parent_name})` in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L867).
3. Treat ambiguous bases as unresolved instead of linking to arbitrary matches.

Acceptance criteria:

1. Inheritance linking does not perform graph-wide name-only lookups.
2. Ambiguity is surfaced as data, not hidden by guessed edges.

## Phase 3: Batch Database Writes

### 5. Replace per-node and per-parameter `session.run(...)` loops with `UNWIND` batches

The heaviest write loop is in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L299).

Required changes:

1. Collect functions, classes, variables, parameters, imports, and relationships into arrays.
2. Use `UNWIND $rows AS row` queries for each node type and relationship type.
3. Keep batch sizes bounded, for example 250 to 1000 rows, to avoid oversized transactions.
4. Move from one query per parameter to one batched parameter upsert and one batched `HAS_PARAMETER` creation.

Reasoning:

The current design spends too much time on network, parser, and planner overhead for tiny queries.

Acceptance criteria:

1. The number of DB round trips per file drops by at least an order of magnitude.
2. Indexing throughput remains stable deeper into large repositories.

### 6. Batch CALLS and INHERITS edge creation separately from resolution

Required changes:

1. Have the resolver produce normalized edge rows.
2. Persist those rows with batched `UNWIND` statements.
3. Deduplicate edges in memory before writing.

Acceptance criteria:

1. Relationship creation cost is dominated by edge count, not retry count.
2. Duplicate `MERGE` churn is reduced materially.

## Phase 4: Fix Schema and Index Mismatches

### 7. Add missing constraints and indexes for actual `MERGE` patterns

The current schema setup in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L139) does not cover every hot lookup path.

Required changes:

1. Add a uniqueness constraint or equivalent indexed key for `Parameter` nodes keyed by the exact merge pattern used.
2. Review all `MERGE` and `MATCH` shapes in `graph_builder.py` and ensure each hot path has a supporting index.
3. If any broad name-only lookup remains, add an explicit non-unique `Function(name)` index and document why it still exists.
4. Prefer query shapes that match the existing composite constraints, for example always including `path` when available.

Acceptance criteria:

1. Every repeated `MERGE` pattern in the indexer has a matching index or constraint.
2. No hot query relies on accidental planner behavior.

### 8. Stop using the database for repository-relative path chain creation one segment at a time

Required changes:

1. Build directory path rows up front.
2. Batch directory and `CONTAINS` creation.
3. Reuse cached parent path rows for files in the same directory.

Acceptance criteria:

1. Deep directory trees do not multiply write overhead linearly per file.

## Phase 5: Introduce Observability and Safety Rails

### 9. Add phase timing and query counters

Required changes:

1. Time `pre_scan`, `parse`, `write_nodes`, `link_inheritance`, and `link_calls` separately.
2. Count unresolved calls, ambiguous resolutions, batched rows, and created relationships.
3. Emit a summary at the end of indexing and make it available through job status.

Acceptance criteria:

1. A user can identify where time is being spent without reading source code.
2. Regressions in a single phase are visible immediately.

### 10. Add large-repo guardrails

Required changes:

1. Auto-disable expensive fallback resolution above a configurable file-count threshold.
2. Warn when using the Tree-sitter path on large Go, Java, or TypeScript repositories without SCIP enabled.
3. Add a `fast` mode that skips source storage and unresolved external linking.

Acceptance criteria:

1. Defaults are safe for repositories in the several-hundred-file range.
2. Users receive actionable guidance before the indexer enters a pathological path.

## Phase 6: Prefer SCIP Where It Is Stronger

### 11. Make SCIP the recommended path for large supported repositories

The SCIP gate already exists in [src/codegraphcontext/tools/graph_builder.py](/Users/timothysweet/src/CodeGraphContext/src/codegraphcontext/tools/graph_builder.py#L1178).

Required changes:

1. Improve detection and onboarding for `scip-go`, `scip-typescript`, and other supported indexers.
2. Document SCIP as the default recommendation for large repos.
3. Fall back to Tree-sitter only when SCIP is unavailable.

Acceptance criteria:

1. Supported large repositories avoid heuristic call resolution by default.
2. Tree-sitter remains a compatibility path, not the only scalable option.

## Implementation Order

Implement in this order:

1. Remove or gate global fallback call resolution.
2. Change defaults so expensive external resolution is off.
3. Add instrumentation to verify where time goes before and after changes.
4. Introduce an in-memory symbol table and move call/inheritance resolution onto it.
5. Batch node and relationship writes.
6. Add missing schema support for hot merge patterns, especially `Parameter`.
7. Improve SCIP guidance and large-repo defaults.

## Test Plan

Add targeted tests for:

1. Call resolution with same-name functions across multiple files.
2. No fan-out from unresolved or ambiguous call sites.
3. Parameter upserts with matching uniqueness guarantees.
4. Large synthetic repositories that stress:
   - repeated common function names
   - large numbers of methods
   - nested directories
   - unresolved external calls
5. Performance regression checks that assert query counts or wall-clock ceilings for representative fixture repositories.

## Definition of Done

This work is complete when:

1. `cgc index` completes on repositories with 500 to 2000 files without late-run collapse.
2. Runtime growth is near-linear for representative supported languages.
3. Query counts and phase timings confirm that call-linking is bounded.
4. Ambiguous links are reported explicitly instead of guessed through global name scans.
5. Documentation explains which mode to use for large repositories and why.
