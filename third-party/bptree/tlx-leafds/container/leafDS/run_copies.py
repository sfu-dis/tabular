import os
import tarfile
from subprocess import call
import sys

num_elts = 900
for i in range(1, 8):
	copies_string = "--num_copies="+str(10**i)
	elts_string = "--el_count="+str(num_elts)
	call(["./basic", "--parallel_test=true", copies_string, elts_string])
