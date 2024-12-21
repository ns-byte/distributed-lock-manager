import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from . import lock_pb2
from . import lock_pb2_grpc