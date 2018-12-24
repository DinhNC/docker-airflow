import os
import sys

from my_package_1.my_sub_package_1 import my_module_2

def print_path() -> None:
	#print(f"PYTHONPATH = {os.environ['PYTHONPATH'].split(os.pathsep)}")
	print("sys.path:-\n{}", *(sys.path), sep="\n")
