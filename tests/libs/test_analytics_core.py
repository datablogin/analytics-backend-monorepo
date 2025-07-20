"""Tests for analytics core library."""

import pytest
from libs.analytics_core import __doc__


def test_module_import():
    """Test that analytics_core module can be imported."""
    assert __doc__ == "Analytics core utilities and shared functionality."


def test_module_structure():
    """Test basic module structure."""
    import libs.analytics_core as core
    assert hasattr(core, "__doc__")
    assert core.__doc__ is not None