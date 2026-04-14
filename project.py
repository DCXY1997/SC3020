import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from interface import open_gui

#launch gui in interface.py
if __name__ == "__main__":
    open_gui()
