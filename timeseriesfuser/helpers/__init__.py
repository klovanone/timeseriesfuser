# isort: skip_file
import sys
from pathlib import Path

LOGSPATH = Path(__file__).resolve().parent

from timeseriesfuser.helpers.helpers import isotime, toutcisotime
from timeseriesfuser.core import TimeSeriesFuser
from timeseriesfuser.classes import DataInfo

# use this for helper scripts
rootdir = Path(__file__).resolve().parent.parent
if rootdir not in sys.path:
    sys.path.append(str(rootdir))

# FEATUREFLAG = timeseriesfuser.statics.FEATUREFLAG
