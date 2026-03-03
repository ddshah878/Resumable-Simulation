import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

os.environ.setdefault("SQLITE_DB_PATH", "./test_simulation.db")
