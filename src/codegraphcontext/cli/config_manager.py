"""
Configuration management for CodeGraphContext.
Handles reading, writing, and validating configuration settings.
"""

from pathlib import Path
from typing import Optional, Dict, Any
from rich.console import Console
from rich.table import Table
import os

console = Console()

# Configuration file location
CONFIG_DIR = Path.home() / ".codegraphcontext"
CONFIG_FILE = CONFIG_DIR / ".env"

# Database credential keys (stored in same .env file but not managed as config)
DATABASE_CREDENTIAL_KEYS = {
    "NEO4J_URI",
    "NEO4J_USERNAME",
    "NEO4J_PASSWORD",
    "NEO4J_DATABASE",
}

# Default configuration values
DEFAULT_CONFIG = {
    "DEFAULT_DATABASE": "falkordb",
    "FALKORDB_PATH": str(CONFIG_DIR / "falkordb.db"),
    "FALKORDB_SOCKET_PATH": str(CONFIG_DIR / "falkordb.sock"),
    "INDEX_VARIABLES": "true",
    "ALLOW_DB_DELETION": "false",
    "DEBUG_LOGS": "false",
    "DEBUG_LOG_PATH": str(Path.home() / "mcp_debug.log"),
    "ENABLE_APP_LOGS": "CRITICAL",
    "LIBRARY_LOG_LEVEL": "WARNING",
    "LOG_FILE_PATH": str(CONFIG_DIR / "logs" / "cgc.log"),
    "MAX_FILE_SIZE_MB": "10",
    "IGNORE_TEST_FILES": "false",
    "IGNORE_HIDDEN_FILES": "true",
    "ENABLE_AUTO_WATCH": "false",
    "COMPLEXITY_THRESHOLD": "10",
    "MAX_DEPTH": "unlimited",
    "PARALLEL_WORKERS": "4",
    "CACHE_ENABLED": "true",
    "IGNORE_DIRS": "node_modules,venv,.venv,env,.env,dist,build,target,out,.git,.idea,.vscode,__pycache__",
    "INDEX_SOURCE": "true",
    # SCIP indexer feature flag (default off — existing Tree-sitter behaviour unchanged)
    "SCIP_INDEXER": "false",
    "SCIP_LANGUAGES": "python,typescript,go,rust,java",
    "ENABLE_GLOBAL_FALLBACK_RESOLUTION": "false",
    "SKIP_EXTERNAL_RESOLUTION": "true",
    "INDEX_WRITE_BATCH_SIZE": "500",
    "INDEX_WRITE_BATCH_BYTES": "262144",
    "GLOBAL_FALLBACK_FILE_THRESHOLD": "400",
    "FAST_INDEX_MODE": "false",
}

# Configuration key descriptions
CONFIG_DESCRIPTIONS = {
    "DEFAULT_DATABASE": "Default database backend (neo4j|falkordb|kuzudb)",
    "FALKORDB_PATH": "Path to FalkorDB database file",
    "FALKORDB_SOCKET_PATH": "Path to FalkorDB Unix socket",
    "INDEX_VARIABLES": "Index variable nodes in the graph (lighter graph if false)",
    "ALLOW_DB_DELETION": "Allow full database deletion commands",
    "DEBUG_LOGS": "Enable debug logging (for development/troubleshooting)",
    "DEBUG_LOG_PATH": "Path to debug log file",
    "ENABLE_APP_LOGS": "Application log level (DEBUG|INFO|WARNING|ERROR|CRITICAL|DISABLED)",
    "LIBRARY_LOG_LEVEL": "Log level for third-party libraries (neo4j, asyncio, urllib3) (DEBUG|INFO|WARNING|ERROR|CRITICAL)",
    "LOG_FILE_PATH": "Path to application log file",
    "MAX_FILE_SIZE_MB": "Maximum file size to index (in MB)",
    "IGNORE_TEST_FILES": "Skip test files during indexing",
    "IGNORE_HIDDEN_FILES": "Skip hidden files/directories",
    "ENABLE_AUTO_WATCH": "Automatically watch directory after indexing",
    "COMPLEXITY_THRESHOLD": "Cyclomatic complexity warning threshold",
    "MAX_DEPTH": "Maximum directory depth for indexing (unlimited or number)",
    "PARALLEL_WORKERS": "Number of parallel indexing workers",
    "CACHE_ENABLED": "Enable caching for faster re-indexing",
    "IGNORE_DIRS": "Comma-separated list of directory names to ignore during indexing",
    "INDEX_SOURCE": "Store full source code in graph database (for faster indexing use false, for better performance use true)",
    "SCIP_INDEXER": "Use SCIP-based indexing for higher accuracy call/inheritance resolution (requires scip-<lang> tools installed)",
    "SCIP_LANGUAGES": "Comma-separated languages to index via SCIP when SCIP_INDEXER=true (python,typescript,go,rust,java)",
    "ENABLE_GLOBAL_FALLBACK_RESOLUTION": "Allow graph-wide fallback resolution for unresolved calls and inheritance matches (disabled by default for bounded indexing)",
    "SKIP_EXTERNAL_RESOLUTION": "Legacy inverse alias for ENABLE_GLOBAL_FALLBACK_RESOLUTION. When true, graph-wide fallback resolution stays disabled.",
    "INDEX_WRITE_BATCH_SIZE": "Legacy approximate row-count hint for graph write batching",
    "INDEX_WRITE_BATCH_BYTES": "Approximate maximum serialized payload size per graph write batch in bytes",
    "GLOBAL_FALLBACK_FILE_THRESHOLD": "Auto-disable global fallback resolution when indexing at least this many files",
    "FAST_INDEX_MODE": "Skip expensive source storage and use conservative linking defaults for faster indexing",
}

# Valid values for each config key
CONFIG_VALIDATORS = {
    "DEFAULT_DATABASE": ["neo4j", "falkordb", "kuzudb"],
    "INDEX_VARIABLES": ["true", "false"],
    "ALLOW_DB_DELETION": ["true", "false"],
    "DEBUG_LOGS": ["true", "false"],
    "ENABLE_APP_LOGS": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "DISABLED"],
    "LIBRARY_LOG_LEVEL": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    "IGNORE_TEST_FILES": ["true", "false"],
    "IGNORE_HIDDEN_FILES": ["true", "false"],
    "ENABLE_AUTO_WATCH": ["true", "false"],
    "CACHE_ENABLED": ["true", "false"],
    "INDEX_SOURCE": ["true", "false"],
    "SCIP_INDEXER": ["true", "false"],
    "ENABLE_GLOBAL_FALLBACK_RESOLUTION": ["true", "false"],
    "SKIP_EXTERNAL_RESOLUTION": ["true", "false"],
    "FAST_INDEX_MODE": ["true", "false"],
}


def _apply_resolution_aliases(config: Dict[str, str]) -> Dict[str, str]:
    positive = config.get("ENABLE_GLOBAL_FALLBACK_RESOLUTION")
    legacy = config.get("SKIP_EXTERNAL_RESOLUTION")

    if positive is not None:
        config["ENABLE_GLOBAL_FALLBACK_RESOLUTION"] = positive
        config["SKIP_EXTERNAL_RESOLUTION"] = (
            "false" if positive.lower() == "true" else "true"
        )
        return config

    if legacy is not None:
        config["SKIP_EXTERNAL_RESOLUTION"] = legacy
        config["ENABLE_GLOBAL_FALLBACK_RESOLUTION"] = (
            "false" if legacy.lower() == "true" else "true"
        )
        return config

    config["ENABLE_GLOBAL_FALLBACK_RESOLUTION"] = DEFAULT_CONFIG[
        "ENABLE_GLOBAL_FALLBACK_RESOLUTION"
    ]
    config["SKIP_EXTERNAL_RESOLUTION"] = DEFAULT_CONFIG["SKIP_EXTERNAL_RESOLUTION"]
    return config


def ensure_config_dir(path: Path = CONFIG_DIR):
    """
    Ensure that the configuration directory exists.
    Creates the directory and a logs subdirectory if they do not already exist.
    """
    path.mkdir(parents=True, exist_ok=True)
    (path / "logs").mkdir(parents=True, exist_ok=True)


def load_config() -> Dict[str, str]:
    """
    Load configuration with priority support.
    Priority order (highest to lowest):
    1. Environment variables
    2. Local .env file (in current or parent directories)
    3. Global ~/.codegraphcontext/.env

    Note: Does NOT create config directory - caller must call ensure_config_dir() first if needed.
    """
    # Start with defaults
    config = DEFAULT_CONFIG.copy()

    # Load global config
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        config[key.strip()] = value.strip()
        except Exception as e:
            console.print(f"[red]Error loading global config: {e}[/red]")

    # Load local .env file if it exists (overrides global)
    local_env = find_local_env()
    if local_env and local_env.exists():
        try:
            with open(local_env, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        key = key.strip()
                        # Only override if it's a config key (not database credentials in local file)
                        if key in DEFAULT_CONFIG or key in DATABASE_CREDENTIAL_KEYS:
                            config[key] = value.strip()
        except Exception as e:
            console.print(f"[yellow]Warning: Error loading local .env: {e}[/yellow]")

    # Environment variables have highest priority
    for key in DEFAULT_CONFIG.keys():
        env_value = os.getenv(key)
        if env_value is not None:
            config[key] = env_value

    return _apply_resolution_aliases(config)


def find_local_env() -> Optional[Path]:
    """
    Find a local .env file by searching current directory and parents.
    Returns the first .env file found, or None.
    """
    current = Path.cwd()

    # Search up to 5 levels up
    for _ in range(5):
        env_file = current / ".env"
        if env_file.exists() and env_file != CONFIG_FILE:
            return env_file

        # Stop at root
        if current.parent == current:
            break
        current = current.parent

    return None


def save_config(config: Dict[str, str], preserve_db_credentials: bool = True):
    """
    Save configuration to file.
    If preserve_db_credentials is True, existing database credentials will be preserved.
    If preserve_db_credentials is False, credentials from config dict will be written.
    """
    ensure_config_dir()

    # Determine which credentials to write
    credentials_to_write = {}

    if preserve_db_credentials and CONFIG_FILE.exists():
        # Load existing credentials from file to preserve them
        try:
            with open(CONFIG_FILE, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        key = key.strip()
                        if key in DATABASE_CREDENTIAL_KEYS:
                            credentials_to_write[key] = value.strip()
        except Exception:
            pass
    else:
        # Use credentials from the config dict being passed in
        for key in DATABASE_CREDENTIAL_KEYS:
            if key in config:
                credentials_to_write[key] = config[key]

    try:
        with open(CONFIG_FILE, "w") as f:
            f.write("# CodeGraphContext Configuration\n")
            f.write(f"# Location: {CONFIG_FILE}\n\n")

            # Write database credentials first if they exist
            if credentials_to_write:
                f.write("# ===== Database Credentials =====\n")
                for key in sorted(DATABASE_CREDENTIAL_KEYS):
                    if key in credentials_to_write:
                        f.write(f"{key}={credentials_to_write[key]}\n")
                f.write("\n")

            # Write configuration settings
            f.write("# ===== Configuration Settings =====\n")
            for key, value in sorted(config.items()):
                # Skip database credentials (already written above)
                if key in DATABASE_CREDENTIAL_KEYS:
                    continue

                description = CONFIG_DESCRIPTIONS.get(key, "")
                if description:
                    f.write(f"# {description}\n")
                f.write(f"{key}={value}\n\n")

        console.print(f"[green]✅ Configuration saved to {CONFIG_FILE}[/green]")
    except Exception as e:
        console.print(f"[red]Error saving config: {e}[/red]")


def validate_config_value(key: str, value: str) -> tuple[bool, Optional[str]]:
    """
    Validate a configuration value.
    Returns (is_valid, error_message)
    """
    # Skip validation for database credentials (they have their own validation elsewhere)
    if key in DATABASE_CREDENTIAL_KEYS:
        return True, None

    # Strip quotes that might be in the value
    value = value.strip().strip("'\"")

    # Check if key exists
    if key not in DEFAULT_CONFIG:
        available_keys = ", ".join(sorted(DEFAULT_CONFIG.keys()))
        return False, f"Unknown config key: {key}. Available keys: {available_keys}"

    # Validate against specific validators if they exist
    if key in CONFIG_VALIDATORS:
        valid_values = CONFIG_VALIDATORS[key]
        if value.lower() not in [v.lower() for v in valid_values]:
            return (
                False,
                f"Invalid value for {key}. Must be one of: {', '.join(valid_values)}",
            )

    # Specific validation for numeric values
    if key == "MAX_FILE_SIZE_MB":
        try:
            size = int(value)
            if size <= 0:
                return False, "MAX_FILE_SIZE_MB must be a positive number"
        except ValueError:
            return False, "MAX_FILE_SIZE_MB must be a number"

    if key == "COMPLEXITY_THRESHOLD":
        try:
            threshold = int(value)
            if threshold <= 0:
                return False, "COMPLEXITY_THRESHOLD must be a positive number"
        except ValueError:
            return False, "COMPLEXITY_THRESHOLD must be a number"

    if key == "PARALLEL_WORKERS":
        try:
            workers = int(value)
            if workers <= 0 or workers > 32:
                return False, "PARALLEL_WORKERS must be between 1 and 32"
        except ValueError:
            return False, "PARALLEL_WORKERS must be a number"

    if key == "MAX_DEPTH":
        if value.lower() != "unlimited":
            try:
                depth = int(value)
                if depth <= 0:
                    return False, "MAX_DEPTH must be 'unlimited' or a positive number"
            except ValueError:
                return False, "MAX_DEPTH must be 'unlimited' or a number"

    if key == "INDEX_WRITE_BATCH_SIZE":
        try:
            batch_size = int(value)
            if batch_size < 50 or batch_size > 2000:
                return False, "INDEX_WRITE_BATCH_SIZE must be between 50 and 2000"
        except ValueError:
            return False, "INDEX_WRITE_BATCH_SIZE must be a number"

    if key == "INDEX_WRITE_BATCH_BYTES":
        try:
            batch_bytes = int(value)
            if batch_bytes < 16384 or batch_bytes > 16777216:
                return (
                    False,
                    "INDEX_WRITE_BATCH_BYTES must be between 16384 and 16777216",
                )
        except ValueError:
            return False, "INDEX_WRITE_BATCH_BYTES must be a number"

    if key == "GLOBAL_FALLBACK_FILE_THRESHOLD":
        try:
            threshold = int(value)
            if threshold < 50:
                return False, "GLOBAL_FALLBACK_FILE_THRESHOLD must be at least 50"
        except ValueError:
            return False, "GLOBAL_FALLBACK_FILE_THRESHOLD must be a number"

    if key in ("LOG_FILE_PATH", "DEBUG_LOG_PATH"):
        # Validate path is writable
        log_path = Path(value)
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            return False, f"Cannot create log directory: {e}"

    if key in ("FALKORDB_PATH", "FALKORDB_SOCKET_PATH"):
        # Validate path is writable
        db_path = Path(value)
        try:
            db_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            return False, f"Cannot create directory for {key}: {e}"

        # Check if parent directory is writable
        if not os.access(db_path.parent, os.W_OK):
            return False, f"Directory {db_path.parent} is not writable"

    return True, None


def get_config_value(key: str) -> Optional[str]:
    """Get a specific configuration value."""
    config = load_config()
    return config.get(key)


def set_config_value(key: str, value: str) -> bool:
    """Set a configuration value. Returns True if successful."""
    # Ensure config directory exists
    ensure_config_dir()

    # Validate
    is_valid, error_msg = validate_config_value(key, value)
    if not is_valid:
        console.print(f"[red]❌ {error_msg}[/red]")
        return False

    # Load, update, and save
    config = load_config()
    config[key] = value
    if key == "ENABLE_GLOBAL_FALLBACK_RESOLUTION":
        config["SKIP_EXTERNAL_RESOLUTION"] = (
            "false" if value.lower() == "true" else "true"
        )
    elif key == "SKIP_EXTERNAL_RESOLUTION":
        config["ENABLE_GLOBAL_FALLBACK_RESOLUTION"] = (
            "false" if value.lower() == "true" else "true"
        )
    save_config(config)

    console.print(f"[green]✅ Set {key} = {value}[/green]")
    return True


def reset_config():
    """Reset configuration to defaults (preserves database credentials)."""
    ensure_config_dir()
    save_config(DEFAULT_CONFIG.copy(), preserve_db_credentials=True)
    console.print("[green]✅ Configuration reset to defaults[/green]")
    console.print("[cyan]Note: Database credentials were preserved[/cyan]")


def ensure_config_file():
    """
    Create default .env config file on first run if it does not exist.
    """
    ensure_config_dir()

    if CONFIG_FILE.exists():
        return False  # file already exists

    save_config(DEFAULT_CONFIG.copy(), preserve_db_credentials=False)
    return True  # file was created


def show_config():
    """Display current configuration in a nice table."""
    created = ensure_config_file()
    if created:
        console.print(
            f"[green]🆕 Created default configuration at {CONFIG_FILE}[/green]\n"
        )
    config = load_config()

    # Separate database credentials from configuration
    db_creds = {k: v for k, v in config.items() if k in DATABASE_CREDENTIAL_KEYS}
    config_settings = {
        k: v for k, v in config.items() if k not in DATABASE_CREDENTIAL_KEYS
    }

    # Show database credentials if they exist
    if db_creds:
        console.print("\n[bold cyan]Database Credentials[/bold cyan]")
        db_table = Table(show_header=True, header_style="bold magenta")
        db_table.add_column("Credential", style="cyan", width=20)
        db_table.add_column("Value", style="green", width=30)

        for key in sorted(db_creds.keys()):
            value = db_creds[key]
            # Mask password
            if "PASSWORD" in key:
                value = "********" if value else "(not set)"
            db_table.add_row(key, value)

        console.print(db_table)

    # Show configuration settings
    console.print("\n[bold cyan]Configuration Settings[/bold cyan]")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Setting", style="cyan", width=25)
    table.add_column("Value", style="green", width=20)
    table.add_column("Description", style="dim", width=50)

    for key in sorted(config_settings.keys()):
        value = config_settings[key]
        description = CONFIG_DESCRIPTIONS.get(key, "")

        # Highlight non-default values
        if value != DEFAULT_CONFIG.get(key):
            value_style = "[bold yellow]" + value + "[/bold yellow]"
        else:
            value_style = value

        table.add_row(key, value_style, description)

    console.print(table)
    console.print(f"\n[cyan]Config file: {CONFIG_FILE}[/cyan]")
