from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, Optional

SELF_CALL_PREFIXES = {"self", "this", "super", "super()", "cls", "@"}
TYPE_LABELS = {"Class", "Struct", "Record", "Interface", "Trait"}
CALLABLE_TARGET_LABELS = {"Function", "Class"}
CONSTRUCTOR_NAMES = {"__init__", "constructor"}


@dataclass(frozen=True)
class SymbolRef:
    label: str
    name: str
    path: str
    line_number: Optional[int] = None
    class_context: Optional[str] = None


@dataclass(frozen=True)
class ResolvedCallEdge:
    caller_label: str
    caller_name: Optional[str]
    caller_path: str
    caller_line_number: Optional[int]
    callee_label: str
    callee_name: str
    callee_path: str
    callee_line_number: Optional[int]
    line_number: int
    args: list[str]
    full_call_name: str


@dataclass(frozen=True)
class ResolvedTypeEdge:
    relation: str
    child_label: str
    child_name: str
    child_path: str
    child_line_number: Optional[int]
    parent_label: str
    parent_name: str
    parent_path: str
    parent_line_number: Optional[int]


@dataclass
class IndexingMetrics:
    counters: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    phase_timings: Dict[str, float] = field(default_factory=dict)

    def increment(self, key: str, amount: int = 1) -> None:
        self.counters[key] = self.counters.get(key, 0) + amount

    def add_timing(self, phase: str, seconds: float) -> None:
        self.phase_timings[phase] = round(seconds, 4)

    def summary(self) -> Dict[str, Any]:
        return {
            "counters": dict(sorted(self.counters.items())),
            "phase_timings": dict(sorted(self.phase_timings.items())),
        }


class SymbolIndex:
    def __init__(self):
        self.by_name: DefaultDict[str, list[SymbolRef]] = defaultdict(list)
        self.by_path_name: DefaultDict[tuple[str, str], list[SymbolRef]] = defaultdict(
            list
        )
        self.by_path_name_line: DefaultDict[tuple[str, str, int], list[SymbolRef]] = (
            defaultdict(list)
        )
        self.methods_by_path_class_name: DefaultDict[
            tuple[str, str, str], list[SymbolRef]
        ] = defaultdict(list)
        self.type_by_path_name: DefaultDict[tuple[str, str], list[SymbolRef]] = (
            defaultdict(list)
        )
        self.constructors_by_path_class: Dict[tuple[str, str], SymbolRef] = {}

    def add(self, symbol: SymbolRef) -> None:
        self.by_name[symbol.name].append(symbol)
        self.by_path_name[(symbol.path, symbol.name)].append(symbol)
        if symbol.line_number is not None:
            self.by_path_name_line[
                (symbol.path, symbol.name, symbol.line_number)
            ].append(symbol)
        if symbol.class_context and symbol.label == "Function":
            self.methods_by_path_class_name[
                (symbol.path, symbol.class_context, symbol.name)
            ].append(symbol)
            if symbol.name in CONSTRUCTOR_NAMES or symbol.name == symbol.class_context:
                self.constructors_by_path_class[(symbol.path, symbol.class_context)] = (
                    symbol
                )
        if symbol.label in TYPE_LABELS:
            self.type_by_path_name[(symbol.path, symbol.name)].append(symbol)

    @classmethod
    def build(cls, all_file_data: Iterable[Dict[str, Any]]) -> "SymbolIndex":
        index = cls()
        mappings = {
            "Function": "functions",
            "Class": "classes",
            "Trait": "traits",
            "Interface": "interfaces",
            "Struct": "structs",
            "Record": "records",
        }
        for file_data in all_file_data:
            file_path = str(Path(file_data["path"]).resolve())
            for label, key in mappings.items():
                for item in file_data.get(key, []):
                    symbol = SymbolRef(
                        label=label,
                        name=item["name"],
                        path=file_path,
                        line_number=item.get("line_number"),
                        class_context=_normalize_class_context(
                            item.get("class_context")
                        ),
                    )
                    index.add(symbol)
        return index

    def unique_by_path_name(
        self, path: str, name: str, labels: Optional[set[str]] = None
    ) -> Optional[SymbolRef]:
        matches = self.by_path_name.get((path, name), [])
        if labels is not None:
            matches = [match for match in matches if match.label in labels]
        return _single_or_none(matches)

    def unique_by_path_name_line(
        self, path: str, name: str, line_number: Optional[int]
    ) -> Optional[SymbolRef]:
        if line_number is None:
            return None
        return _single_or_none(
            self.by_path_name_line.get((path, name, line_number), [])
        )

    def unique_method(
        self, path: str, class_name: Optional[str], name: str
    ) -> Optional[SymbolRef]:
        if not class_name:
            return None
        return _single_or_none(
            self.methods_by_path_class_name.get((path, class_name, name), [])
        )

    def unique_global(
        self, name: str, labels: Optional[set[str]] = None
    ) -> Optional[SymbolRef]:
        matches = self.by_name.get(name, [])
        if labels is not None:
            matches = [match for match in matches if match.label in labels]
        return _single_or_none(matches)

    def constructor_for(self, symbol: SymbolRef) -> Optional[SymbolRef]:
        if symbol.label not in TYPE_LABELS:
            return None
        return self.constructors_by_path_class.get((symbol.path, symbol.name))


def collect_call_edges(
    all_file_data: Iterable[Dict[str, Any]],
    imports_map: Dict[str, list[str]],
    symbol_index: SymbolIndex,
    allow_global_fallback: bool,
    metrics: Optional[IndexingMetrics] = None,
) -> list[ResolvedCallEdge]:
    edges: list[ResolvedCallEdge] = []
    seen: set[tuple[Any, ...]] = set()
    for file_data in all_file_data:
        file_edges = resolve_calls_for_file(
            file_data, imports_map, symbol_index, allow_global_fallback, metrics
        )
        for edge in file_edges:
            edge_key = (
                edge.caller_label,
                edge.caller_name,
                edge.caller_path,
                edge.caller_line_number,
                edge.callee_label,
                edge.callee_name,
                edge.callee_path,
                edge.callee_line_number,
                edge.line_number,
                edge.full_call_name,
            )
            if edge_key in seen:
                continue
            seen.add(edge_key)
            edges.append(edge)
    return edges


def resolve_calls_for_file(
    file_data: Dict[str, Any],
    imports_map: Dict[str, list[str]],
    symbol_index: SymbolIndex,
    allow_global_fallback: bool,
    metrics: Optional[IndexingMetrics] = None,
) -> list[ResolvedCallEdge]:
    caller_file_path = str(Path(file_data["path"]).resolve())
    local_imports = build_local_imports(file_data)
    local_names = {
        item["name"]
        for collection in (
            "functions",
            "classes",
            "interfaces",
            "traits",
            "structs",
            "records",
        )
        for item in file_data.get(collection, [])
    }
    edges: list[ResolvedCallEdge] = []
    for call in file_data.get("function_calls", []):
        resolved = resolve_call(
            file_data=file_data,
            call=call,
            caller_file_path=caller_file_path,
            local_names=local_names,
            local_imports=local_imports,
            imports_map=imports_map,
            symbol_index=symbol_index,
            allow_global_fallback=allow_global_fallback,
            metrics=metrics,
        )
        if resolved is not None:
            edges.append(resolved)
    return edges


def collect_inheritance_edges(
    all_file_data: Iterable[Dict[str, Any]],
    imports_map: Dict[str, list[str]],
    symbol_index: SymbolIndex,
    allow_global_fallback: bool,
    metrics: Optional[IndexingMetrics] = None,
) -> list[ResolvedTypeEdge]:
    edges: list[ResolvedTypeEdge] = []
    seen: set[tuple[Any, ...]] = set()
    for file_data in all_file_data:
        file_edges = resolve_inheritance_for_file(
            file_data, imports_map, symbol_index, allow_global_fallback, metrics
        )
        for edge in file_edges:
            edge_key = (
                edge.relation,
                edge.child_label,
                edge.child_name,
                edge.child_path,
                edge.parent_label,
                edge.parent_name,
                edge.parent_path,
            )
            if edge_key in seen:
                continue
            seen.add(edge_key)
            edges.append(edge)
    return edges


def resolve_inheritance_for_file(
    file_data: Dict[str, Any],
    imports_map: Dict[str, list[str]],
    symbol_index: SymbolIndex,
    allow_global_fallback: bool,
    metrics: Optional[IndexingMetrics] = None,
) -> list[ResolvedTypeEdge]:
    file_path = str(Path(file_data["path"]).resolve())
    local_imports = build_local_imports(file_data)
    local_type_names = {
        item["name"]
        for collection in ("classes", "interfaces", "traits", "structs", "records")
        for item in file_data.get(collection, [])
    }
    edges: list[ResolvedTypeEdge] = []
    mappings = [
        ("classes", "Class"),
        ("interfaces", "Interface"),
        ("structs", "Struct"),
        ("records", "Record"),
    ]
    for list_name, child_label in mappings:
        for type_item in file_data.get(list_name, []):
            bases = type_item.get("bases") or []
            for index, base_expr in enumerate(bases):
                base_name = _clean_type_name(base_expr)
                if not base_name or base_name == "object":
                    continue
                parent_symbol = _resolve_type_symbol(
                    base_expr=base_expr,
                    base_name=base_name,
                    caller_file_path=file_path,
                    local_type_names=local_type_names,
                    local_imports=local_imports,
                    imports_map=imports_map,
                    symbol_index=symbol_index,
                    allow_global_fallback=allow_global_fallback,
                )
                if parent_symbol is None:
                    if metrics is not None:
                        metrics.increment("unresolved_inheritance")
                    continue
                relation = "INHERITS"
                if child_label in {"Class", "Struct", "Record"} and (
                    index > 0 or parent_symbol.label == "Interface"
                ):
                    relation = "IMPLEMENTS"
                edges.append(
                    ResolvedTypeEdge(
                        relation=relation,
                        child_label=child_label,
                        child_name=type_item["name"],
                        child_path=file_path,
                        child_line_number=type_item.get("line_number"),
                        parent_label=parent_symbol.label,
                        parent_name=parent_symbol.name,
                        parent_path=parent_symbol.path,
                        parent_line_number=parent_symbol.line_number,
                    )
                )
                if metrics is not None:
                    metrics.increment("resolved_inheritance")
    return edges


def resolve_call(
    *,
    file_data: Dict[str, Any],
    call: Dict[str, Any],
    caller_file_path: str,
    local_names: set[str],
    local_imports: Dict[str, str],
    imports_map: Dict[str, list[str]],
    symbol_index: SymbolIndex,
    allow_global_fallback: bool,
    metrics: Optional[IndexingMetrics] = None,
) -> Optional[ResolvedCallEdge]:
    called_name = call.get("name")
    if not called_name:
        return None
    builtins_obj = __builtins__
    builtin_names = (
        set(builtins_obj) if isinstance(builtins_obj, dict) else set(dir(builtins_obj))
    )
    if called_name in builtin_names:
        return None

    full_call_name = call.get("full_name") or called_name
    base_obj = full_call_name.split(".")[0] if "." in full_call_name else None
    is_chained_self_call = (
        full_call_name.count(".") > 1 and base_obj in SELF_CALL_PREFIXES
        if base_obj
        else False
    )
    class_context = _normalize_class_context(call.get("class_context"))

    callee_symbol: Optional[SymbolRef] = None
    if base_obj in SELF_CALL_PREFIXES and class_context:
        callee_symbol = symbol_index.unique_method(
            caller_file_path, class_context, called_name
        )

    if callee_symbol is None and called_name in local_names:
        callee_symbol = _prefer_function(
            symbol_index.by_path_name.get((caller_file_path, called_name), []),
            symbol_index,
        )

    lookup_name = called_name
    if callee_symbol is None and call.get("inferred_obj_type"):
        lookup_name = str(call["inferred_obj_type"]).split(".")[-1]
        resolved_path = _resolve_import_path(
            lookup_name, called_name, local_imports, imports_map
        )
        if resolved_path:
            callee_symbol = _prefer_function(
                symbol_index.by_path_name.get((resolved_path, called_name), []),
                symbol_index,
            )

    if callee_symbol is None and base_obj and base_obj not in SELF_CALL_PREFIXES:
        resolved_path = _resolve_import_path(
            base_obj, called_name, local_imports, imports_map
        )
        if resolved_path:
            callee_symbol = _prefer_function(
                symbol_index.by_path_name.get((resolved_path, called_name), []),
                symbol_index,
            )

    if callee_symbol is None:
        resolved_path = _resolve_import_path(
            called_name, called_name, local_imports, imports_map
        )
        if resolved_path:
            callee_symbol = _prefer_function(
                symbol_index.by_path_name.get((resolved_path, called_name), []),
                symbol_index,
            )

    if callee_symbol is None and not is_chained_self_call:
        local_symbol = symbol_index.unique_by_path_name(
            caller_file_path, called_name, labels=CALLABLE_TARGET_LABELS
        )
        if local_symbol is not None:
            callee_symbol = _final_call_target(local_symbol, symbol_index)

    if callee_symbol is None and allow_global_fallback:
        global_symbol = symbol_index.unique_global(
            called_name, labels=CALLABLE_TARGET_LABELS
        )
        if global_symbol is not None:
            callee_symbol = _final_call_target(global_symbol, symbol_index)

    if callee_symbol is None:
        if metrics is not None:
            metrics.increment("unresolved_calls")
        return None

    caller_symbol = _resolve_caller_symbol(
        call.get("context"), caller_file_path, symbol_index
    )
    edge = ResolvedCallEdge(
        caller_label=caller_symbol.label if caller_symbol else "File",
        caller_name=caller_symbol.name if caller_symbol else None,
        caller_path=caller_file_path,
        caller_line_number=caller_symbol.line_number if caller_symbol else None,
        callee_label=callee_symbol.label,
        callee_name=callee_symbol.name,
        callee_path=callee_symbol.path,
        callee_line_number=callee_symbol.line_number,
        line_number=call.get("line_number", 0),
        args=_normalize_call_args(call.get("args", [])),
        full_call_name=full_call_name,
    )
    if metrics is not None:
        metrics.increment("resolved_calls")
    return edge


def build_local_imports(file_data: Dict[str, Any]) -> Dict[str, str]:
    local_imports: Dict[str, str] = {}
    for imp in file_data.get("imports", []):
        name = imp.get("name") or imp.get("source")
        if not name:
            continue
        alias = imp.get("alias") or name.split(".")[-1]
        local_imports[alias] = name
    return local_imports


def _resolve_caller_symbol(
    context: Any, caller_file_path: str, symbol_index: SymbolIndex
) -> Optional[SymbolRef]:
    if not isinstance(context, (list, tuple)) or len(context) < 3 or context[0] is None:
        return None
    context_name = context[0]
    context_line = context[2]
    exact = symbol_index.unique_by_path_name_line(
        caller_file_path, context_name, context_line
    )
    if exact is not None:
        return exact
    return symbol_index.unique_by_path_name(
        caller_file_path, context_name, labels={"Function", "Class"}
    )


def _resolve_type_symbol(
    *,
    base_expr: str,
    base_name: str,
    caller_file_path: str,
    local_type_names: set[str],
    local_imports: Dict[str, str],
    imports_map: Dict[str, list[str]],
    symbol_index: SymbolIndex,
    allow_global_fallback: bool,
) -> Optional[SymbolRef]:
    if "." in base_expr:
        prefix = _clean_type_name(base_expr.split(".")[0])
        resolved_path = _resolve_import_path(
            prefix, base_name, local_imports, imports_map
        )
        if resolved_path:
            resolved = symbol_index.unique_by_path_name(
                resolved_path, base_name, labels=TYPE_LABELS
            )
            if resolved is not None:
                return resolved
    if base_name in local_type_names:
        local = symbol_index.unique_by_path_name(
            caller_file_path, base_name, labels=TYPE_LABELS
        )
        if local is not None:
            return local
    resolved_path = _resolve_import_path(
        base_name, base_name, local_imports, imports_map
    )
    if resolved_path:
        resolved = symbol_index.unique_by_path_name(
            resolved_path, base_name, labels=TYPE_LABELS
        )
        if resolved is not None:
            return resolved
    if allow_global_fallback:
        return symbol_index.unique_global(base_name, labels=TYPE_LABELS)
    return None


def _resolve_import_path(
    lookup_name: str,
    called_name: str,
    local_imports: Dict[str, str],
    imports_map: Dict[str, list[str]],
) -> Optional[str]:
    explicit_import = local_imports.get(lookup_name)
    if explicit_import:
        direct_paths = imports_map.get(explicit_import, [])
        if len(direct_paths) == 1:
            return direct_paths[0]
        suffix = explicit_import.replace(".", "/")
        candidates = imports_map.get(called_name, []) or imports_map.get(
            lookup_name, []
        )
        matching = [candidate for candidate in candidates if suffix in candidate]
        if len(matching) == 1:
            return matching[0]
    possible_paths = imports_map.get(lookup_name, [])
    if len(possible_paths) == 1:
        return possible_paths[0]
    return None


def _final_call_target(
    symbol: SymbolRef, symbol_index: SymbolIndex
) -> Optional[SymbolRef]:
    constructor = symbol_index.constructor_for(symbol)
    if constructor is not None:
        return constructor
    return symbol if symbol.label in CALLABLE_TARGET_LABELS else None


def _prefer_function(
    matches: list[SymbolRef], symbol_index: SymbolIndex
) -> Optional[SymbolRef]:
    matches = [match for match in matches if match.label in CALLABLE_TARGET_LABELS]
    if not matches:
        return None
    function_matches = [match for match in matches if match.label == "Function"]
    if len(function_matches) == 1:
        return function_matches[0]
    type_matches = [match for match in matches if match.label == "Class"]
    if len(type_matches) == 1:
        return _final_call_target(type_matches[0], symbol_index)
    return _single_or_none(matches)


def _clean_type_name(value: str) -> str:
    return value.split("<", 1)[0].split("[", 1)[0].split(".")[-1].strip()


def _normalize_class_context(value: Any) -> Optional[str]:
    if isinstance(value, (list, tuple)):
        return value[0] if value and value[0] else None
    return value or None


def _normalize_call_args(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (str, bytes)):
        return [_stringify_call_arg(value)]
    if not isinstance(value, (list, tuple)):
        return [_stringify_call_arg(value)]
    return [_stringify_call_arg(item) for item in value if item is not None]


def normalize_args_for_storage(value: Any) -> list[str]:
    return _normalize_call_args(value)


def normalize_parameter_name(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("name", "param", "identifier"):
            candidate = value.get(key)
            if candidate:
                return _stringify_call_arg(candidate)
    return _stringify_call_arg(value)


def _stringify_call_arg(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    return json.dumps(value, sort_keys=True, default=str)


def _single_or_none(matches: list[SymbolRef]) -> Optional[SymbolRef]:
    if len(matches) != 1:
        return None
    return matches[0]
