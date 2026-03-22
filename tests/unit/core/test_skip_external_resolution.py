"""Unit tests for conservative fallback-resolution configuration behavior."""

import inspect
import os
from unittest.mock import patch

from codegraphcontext.cli.config_manager import (
    CONFIG_DESCRIPTIONS,
    CONFIG_VALIDATORS,
    DEFAULT_CONFIG,
    get_config_value,
    set_config_value,
)


class TestFallbackResolutionConfig:
    def test_positive_flag_exists(self):
        assert "ENABLE_GLOBAL_FALLBACK_RESOLUTION" in CONFIG_DESCRIPTIONS
        assert "ENABLE_GLOBAL_FALLBACK_RESOLUTION" in CONFIG_VALIDATORS

    def test_legacy_flag_exists(self):
        assert "SKIP_EXTERNAL_RESOLUTION" in CONFIG_DESCRIPTIONS
        assert "SKIP_EXTERNAL_RESOLUTION" in CONFIG_VALIDATORS

    def test_defaults_are_conservative(self):
        assert DEFAULT_CONFIG["ENABLE_GLOBAL_FALLBACK_RESOLUTION"] == "false"
        assert DEFAULT_CONFIG["SKIP_EXTERNAL_RESOLUTION"] == "true"

    def test_setting_positive_flag_updates_legacy_inverse(self):
        set_config_value("ENABLE_GLOBAL_FALLBACK_RESOLUTION", "true")
        assert get_config_value("ENABLE_GLOBAL_FALLBACK_RESOLUTION") == "true"
        assert get_config_value("SKIP_EXTERNAL_RESOLUTION") == "false"

    def test_setting_legacy_flag_updates_positive_inverse(self):
        set_config_value("SKIP_EXTERNAL_RESOLUTION", "true")
        assert get_config_value("SKIP_EXTERNAL_RESOLUTION") == "true"
        assert get_config_value("ENABLE_GLOBAL_FALLBACK_RESOLUTION") == "false"

    def test_environment_override_supports_positive_flag(self):
        with patch.dict(
            os.environ, {"ENABLE_GLOBAL_FALLBACK_RESOLUTION": "true"}, clear=True
        ):
            assert get_config_value("ENABLE_GLOBAL_FALLBACK_RESOLUTION") == "true"
            assert get_config_value("SKIP_EXTERNAL_RESOLUTION") == "false"


class TestGraphBuilderFallbackBehavior:
    def test_graph_builder_uses_positive_flag_helper(self):
        from codegraphcontext.tools.graph_builder import GraphBuilder

        source = inspect.getsource(GraphBuilder._global_fallback_enabled)
        assert "ENABLE_GLOBAL_FALLBACK_RESOLUTION" in source
        assert "SKIP_EXTERNAL_RESOLUTION" in source

    def test_graph_builder_large_repo_guardrail_exists(self):
        from codegraphcontext.tools.graph_builder import GraphBuilder

        source = inspect.getsource(GraphBuilder._get_large_repo_threshold)
        assert "GLOBAL_FALLBACK_FILE_THRESHOLD" in source

        build_source = inspect.getsource(GraphBuilder.build_graph_from_path_async)
        assert "guardrail_disabled_global_fallback" in build_source
