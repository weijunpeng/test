from pytoolkit.provider import DataProvider
from pytoolkit.provider import TDWProvider
from pytoolkit.provider import TDWSQLProvider
from pytoolkit.provider import TDBankProvider
from pytoolkit.provider import TubeReceiverConfig
from pytoolkit.provider import HippoReceiverConfig
from pytoolkit.funcs import TubeSenderConfig
from pytoolkit.ddl import TDWUtil
from pytoolkit.ddl import TableDesc
from pytoolkit.ddl import TableInfo

"""
toolkit package is the Python API for tdw-spark-toolkit.

Public classes:

  - :class:`DataProvider`:
      Base class for DATA ACCESS API.
  - :class:`TDWProvider`:
      entry point for TDW-RDD API.
  - :class:`TDWSQLProvider`:
      entry point for TDW-DARAFRAME API.
  - :class:`TDWUtil`:
      entry point for TDW-DDL API.
  - :class:`TDBankProvider`:
      entry point for TDABANK-DSTREAM API.
  - :class:`TableInfo`:
      meta info for tdw table.
  - :class:`TableDesc`:
      table desc for create table.

"""

__author__ = 'sharkdtu'

__all__ = [
    "DataProvider", "TDWProvider", "TDWSQLProvider",
    "TDBankProvider", "TDWUtil", "TableInfo",
    "TableDesc", "TubeReceiverConfig", "HippoReceiverConfig",
    "TubeSenderConfig"
]
