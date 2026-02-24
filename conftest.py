import sys, os
# Ensure project root on sys.path for tests
ROOT = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.abspath(os.path.join(ROOT))
if PARENT not in sys.path:
    sys.path.insert(0, PARENT)
if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())
