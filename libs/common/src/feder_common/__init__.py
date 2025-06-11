"""Code shared between the Feder public API and backend.

The parts of this module relevant to the public API are:

 - `feder_common.models`: Model classes for trajectories and trajectory points.
 - `feder_common.db`: Database access code.
"""
from .models import *  # noqa
from .db import *  # noqa
from .utils import *  # noqa
from .version import *  # noqa

__version__ = '0.1.12'
