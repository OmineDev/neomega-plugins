"""Versioned author API for the optional neomega.display provider."""
from .client import (
    DisplayCall, DisplayCallError, DisplayClient, DisplayReceipt,
    DisplayRejected, DisplayUncertain, MIN_PROVIDER_VERSION, PROVIDER_ID, SERVICE_MAJOR,
)
from .types import DisplaySpec, Spin, Static, WorldPosition

__version__ = '1.1.0'
__all__ = [
    'DisplayCall', 'DisplayCallError', 'DisplayClient', 'DisplayReceipt',
    'DisplayRejected', 'DisplayUncertain', 'DisplaySpec', 'Spin', 'Static',
    'WorldPosition', 'PROVIDER_ID', 'SERVICE_MAJOR', 'MIN_PROVIDER_VERSION',
]
