"""
BIDS Sidecar classes for Pennsieve data migration.

This package contains classes for generating BIDS-compliant sidecar files
(both JSON and TSV formats) for iEEG datasets.
"""

from .base import Sidecar, JSONSidecar, TSVSidecar
from .channels import ChannelsSidecar
from .coordsystem import CoordSystemSidecar
from .dataset_description import DatasetDescriptionSidecar
from .eeg import EEGSidecar
from .electrodes import ElectrodesSidecar
from .events import EventsSidecar
from .ieeg import IeegSidecar
from .participants import ParticipantsSidecar
from .participants_tsv import ParticipantsSideCarTSV
from .sessions import SessionSidecar

__all__ = [
    # Base classes
    "Sidecar",
    "JSONSidecar",
    "TSVSidecar",
    # JSON sidecars
    "CoordSystemSidecar",
    "DatasetDescriptionSidecar",
    "EEGSidecar",
    "IeegSidecar",
    "ParticipantsSidecar",
    # TSV sidecars
    "ChannelsSidecar",
    "ElectrodesSidecar",
    "EventsSidecar",
    "ParticipantsSideCarTSV",
    "SessionSidecar",
]
