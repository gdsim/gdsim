# GDSim: a platform for benchmarking geo-distributed job schedulers

This program/library is meant to allow for easier first-level comparison of job schedulers for geo-distributed data centers.
It was created because we found no suitable simulators for comparing job schedulers for geo-distributed data centers.
This does not substitute building a test bed for that comparison, but that is a costly solution that can be used later.

## How to install

Run `go install` from the root directory of this project.

## How to use

To run the simulator, call `gdsim trace.jobs`, where `trace.jobs` is a file describing the workload.
The simulator will look for a topology description file at `default.topo`, and a file describing the files available at the data centers at `trace.files`.
Both of those can be changed with the options `-topology` and `-files`, respectively.
By default it will use the Global-SRPT scheduler, use the `-scheduler` option to change that:
currently implemented alternatives are `SWAG` and `GEODIS`.

## Files format

This section describe the format used in the files.
This format was selected for ease of initial implementation, but does not fully correspond to what can be implemented using the simulator's library.

### Job trace file format

Each line corresponds to a job, with five or more space separated fields:

 1. Job ID;
 2. Number of cores required for execution;
 3. Submission delay in seconds for this job, after the submission of the previous job (inter-arrival delay);
 4. File ID, for the file that is required for the execution of this job. The file is described in the file trace;
 5. 5th field and following: duration in seconds of each task required for the completion of the job.

### File trace file format

Each line corresponds to a file, with three or more space separated fields:

 1. File ID;
 2. Size of the file in bytes;
 3. 3rd and following: data centers that have a copy of the file. 0 means the first data center, 1 means the second, and so on. The highest number must not exceed the amount of available data centers

### Topology file format

The first line will have a single positive integer n, the number of data centers.
The next n lines will have each a pair of positive integer, the first for the number of computers in the corresponding data center, the second for the number of cores in each computer (while the simulator does not enforce that all computers have to be the same, This was simpler for the frontend).
Those are followed by another n lines, each of each containing n positive integers, forming an n by n matrix of bandwidth from one data center to another.
Bandwidth is measured in b/s.
The value indicating from a data center to itself is read but not used.
