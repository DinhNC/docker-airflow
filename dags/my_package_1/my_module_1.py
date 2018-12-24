import os
import sys

def print_path() -> None:
	#print(f"PYTHONPATH = {os.environ['PYTHONPATH'].split(os.pathsep)}")
	print("sys.path:-\n{}", *(sys.path), sep="\n")
