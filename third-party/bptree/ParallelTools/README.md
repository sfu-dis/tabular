# ParallelTools
This repository seeks to make it easy for do perform simple parallelism and staying away from any of the details of the different parallel runtimes.

It is currently only set of for [opencilk](https://www.opencilk.org/) and [parlaylib](https://cmuparlay.github.io/parlaylib/), but is general enough that other runtimes can easily be added.  It assume that the parallel libraries have already been installed 

To run in parallel simple define `CILK=1` or `PARLAY=1` before the inclusion of any of the files.  Without this, the code will all still run correctly with the same behavior, but it will only run serially. 
