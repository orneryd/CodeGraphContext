# cgc.spec
# PyInstaller build spec for CodeGraphContext (Linux x86_64)
#
# Build with:
#   .venv/bin/pyinstaller cgc.spec --clean
#
# Output: dist/cgc  (single self-contained binary)
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

import sys
import os
from pathlib import Path

block_cipher = None

# ── Locate the venv site-packages ──────────────────────────────────────────
site_packages = Path('.venv/lib/python3.12/site-packages')

# ── 1. Binary .so files to bundle ──────────────────────────────────────────
binaries = []

# tree-sitter core binding
ts_core = site_packages / 'tree_sitter' / '_binding.cpython-312-x86_64-linux-gnu.so'
if ts_core.exists():
    binaries.append((str(ts_core), 'tree_sitter'))

# tree-sitter-language-pack: ALL language .so bindings
ts_pack_bindings = site_packages / 'tree_sitter_language_pack' / 'bindings'
if ts_pack_bindings.exists():
    for so_file in ts_pack_bindings.glob('*.so'):
        binaries.append((str(so_file), 'tree_sitter_language_pack/bindings'))

# tree-sitter-yaml binding
ts_yaml = site_packages / 'tree_sitter_yaml' / '_binding.abi3.so'
if ts_yaml.exists():
    binaries.append((str(ts_yaml), 'tree_sitter_yaml'))

# tree-sitter-embedded-template binding
ts_emb = site_packages / 'tree_sitter_embedded_template' / '_binding.abi3.so'
if ts_emb.exists():
    binaries.append((str(ts_emb), 'tree_sitter_embedded_template'))

# tree-sitter-c-sharp binding
ts_cs = site_packages / 'tree_sitter_c_sharp' / '_binding.abi3.so'
if ts_cs.exists():
    binaries.append((str(ts_cs), 'tree_sitter_c_sharp'))

# KùzuDB native extension
for kuzu_so in (site_packages / 'kuzu').glob('*.so'):
    binaries.append((str(kuzu_so), 'kuzu'))

# FalkorDB Lite (redislite) native binary
redis_bin = site_packages / 'redislite' / 'bin'
if redis_bin.exists():
    for f in redis_bin.iterdir():
        binaries.append((str(f), 'redislite/bin'))

falkordblite_scripts = site_packages / 'falkordblite.scripts'
if falkordblite_scripts.exists():
    for f in falkordblite_scripts.glob('*.so'):
        binaries.append((str(f), 'falkordblite.scripts'))

# ── 2. Data files ────────────────────────────────────────────────────────────
datas = []

# stdlibs: dynamically imports py3.py, py312.py, etc. via importlib — must be data files
stdlibs_dir = site_packages / 'stdlibs'
if stdlibs_dir.exists():
    for py_file in stdlibs_dir.glob('*.py'):
        datas.append((str(py_file), 'stdlibs'))

# mcp package data files
datas += collect_data_files('mcp', includes=['**/*'])

# mcp.json shipped with the package
mcp_json = Path('src/codegraphcontext/mcp.json')
if mcp_json.exists():
    datas.append((str(mcp_json), 'codegraphcontext'))

# tree-sitter-language-pack Python metadata files
ts_pack_dir = site_packages / 'tree_sitter_language_pack'
if ts_pack_dir.exists():
    for f in ts_pack_dir.glob('*.py'):
        datas.append((str(f), 'tree_sitter_language_pack'))
    for f in ts_pack_dir.glob('*.pyi'):
        datas.append((str(f), 'tree_sitter_language_pack'))

# redislite config/data files needed by falkordb worker subprocess
redislite_dir = site_packages / 'redislite'
if redislite_dir.exists():
    for f in redislite_dir.glob('*.conf'):
        datas.append((str(f), 'redislite'))

# ── 3. Hidden imports ────────────────────────────────────────────────────────
hidden_imports = [
    # ── CodeGraphContext internal modules ──
    'codegraphcontext',
    'codegraphcontext.cli',
    'codegraphcontext.cli.main',
    'codegraphcontext.cli.cli_helpers',
    'codegraphcontext.cli.config_manager',
    'codegraphcontext.cli.registry_commands',
    'codegraphcontext.cli.setup_wizard',
    'codegraphcontext.cli.setup_macos',
    'codegraphcontext.cli.visualizer',
    'codegraphcontext.core',
    'codegraphcontext.core.database',
    'codegraphcontext.core.database_falkordb',
    'codegraphcontext.core.database_falkordb_remote',
    'codegraphcontext.core.database_kuzu',
    'codegraphcontext.core.falkor_worker',
    'codegraphcontext.core.jobs',
    'codegraphcontext.core.watcher',
    'codegraphcontext.core.cgc_bundle',
    'codegraphcontext.core.bundle_registry',
    'codegraphcontext.server',
    'codegraphcontext.tool_definitions',
    'codegraphcontext.prompts',
    'codegraphcontext.tools',
    'codegraphcontext.tools.code_finder',
    'codegraphcontext.tools.graph_builder',
    'codegraphcontext.tools.package_resolver',
    'codegraphcontext.tools.system',
    'codegraphcontext.tools.scip_indexer',
    'codegraphcontext.tools.scip_pb2',
    'codegraphcontext.tools.advanced_language_query_tool',
    # language modules
    'codegraphcontext.tools.languages.python',
    'codegraphcontext.tools.languages.javascript',
    'codegraphcontext.tools.languages.typescript',
    'codegraphcontext.tools.languages.typescriptjsx',
    'codegraphcontext.tools.languages.java',
    'codegraphcontext.tools.languages.go',
    'codegraphcontext.tools.languages.rust',
    'codegraphcontext.tools.languages.c',
    'codegraphcontext.tools.languages.cpp',
    'codegraphcontext.tools.languages.ruby',
    'codegraphcontext.tools.languages.php',
    'codegraphcontext.tools.languages.csharp',
    'codegraphcontext.tools.languages.kotlin',
    'codegraphcontext.tools.languages.scala',
    'codegraphcontext.tools.languages.swift',
    'codegraphcontext.tools.languages.haskell',
    'codegraphcontext.tools.languages.dart',
    'codegraphcontext.tools.languages.perl',
    # query toolkits
    'codegraphcontext.tools.query_tool_languages.python_toolkit',
    'codegraphcontext.tools.query_tool_languages.javascript_toolkit',
    'codegraphcontext.tools.query_tool_languages.typescript_toolkit',
    'codegraphcontext.tools.query_tool_languages.java_toolkit',
    'codegraphcontext.tools.query_tool_languages.go_toolkit',
    'codegraphcontext.tools.query_tool_languages.rust_toolkit',
    'codegraphcontext.tools.query_tool_languages.c_toolkit',
    'codegraphcontext.tools.query_tool_languages.cpp_toolkit',
    'codegraphcontext.tools.query_tool_languages.ruby_toolkit',
    'codegraphcontext.tools.query_tool_languages.csharp_toolkit',
    'codegraphcontext.tools.query_tool_languages.scala_toolkit',
    'codegraphcontext.tools.query_tool_languages.swift_toolkit',
    'codegraphcontext.tools.query_tool_languages.haskell_toolkit',
    'codegraphcontext.tools.query_tool_languages.dart_toolkit',
    'codegraphcontext.tools.query_tool_languages.perl_toolkit',
    # handlers
    'codegraphcontext.tools.handlers.analysis_handlers',
    'codegraphcontext.tools.handlers.indexing_handlers',
    'codegraphcontext.tools.handlers.management_handlers',
    'codegraphcontext.tools.handlers.query_handlers',
    'codegraphcontext.tools.handlers.watcher_handlers',
    # utils
    'codegraphcontext.utils.debug_log',
    'codegraphcontext.utils.tree_sitter_manager',
    'codegraphcontext.utils.visualize_graph',

    # ── Third-party ──
    'kuzu',
    'falkordb',
    'redislite',
    'neo4j',
    'neo4j.io',
    'neo4j.auth_management',
    'neo4j.addressing',
    'neo4j.routing',
    'dotenv',
    'typer',
    'typer.core',
    'typer.main',
    'rich',
    'rich.console',
    'rich.table',
    'rich.progress',
    'rich.markup',
    'rich.panel',
    'tree_sitter',
    'tree_sitter_language_pack',
    'tree_sitter_yaml',
    'tree_sitter_embedded_template',
    'tree_sitter_c_sharp',
    'watchdog',
    'watchdog.observers',
    'watchdog.observers.inotify',
    'watchdog.observers.inotify_buffer',
    'watchdog.events',
    'mcp',
    'stdlibs',
    'stdlibs.py3',
    'stdlibs.py312',
    'stdlibs.known',
    'anyio',
    'anyio._backends._asyncio',
    'click',
    'shellingham',
    'httpx',
    'httpcore',
    'importlib.metadata',
    'importlib.util',
    'asyncio',
    'json',
    're',
    'pathlib',
    'threading',
    'subprocess',
    'socket',
    'atexit',
]

# ── 4. Analysis ──────────────────────────────────────────────────────────────
a = Analysis(
    ['cgc_entry.py'],
    pathex=[
        'src',
        str(site_packages),
    ],
    binaries=binaries,
    datas=datas,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter', '_tkinter',
        'matplotlib', 'numpy', 'pandas', 'scipy',
        'PIL', 'cv2', 'torch', 'tensorflow',
        'jupyter', 'notebook', 'IPython',
        'pydoc', 'doctest', 'xmlrpc', 'lib2to3',
        'test', 'unittest.mock',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# ── 5. ONE-FILE EXE ──────────────────────────────────────────────────────────
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='cgc',
    debug=False,
    bootloader_ignore_signals=False,
    strip=True,        # Strip debug symbols → smaller binary
    upx=False,         # Set True if UPX is installed for extra compression
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,      # CLI app — keep console mode
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)
