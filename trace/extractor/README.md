# Data extractor

An utility to create empirical distributions from formatted traces, for use with the generator.

## How to use

The extractor program can be called with two parameters, -jobs and -files:

  -jobs: indicates the job trace file. Default value is out.jobs.
  -files: indicates the file trace file. Default value is out.files.

The program can be executed then as `extractor -jobs out.jobs -files out.files`.
After execution, the program will create multiple .gen and .filegen files, corresponding empirical distributions for job features and file features.
The following are job features extracted in this way:

  - number of cpus required (cpuTrace)
  - inter-arrival delay (delayTrace)
  - task duration (durationTrace)
  - required file for execution (fileTrace)
  - number of tasks per job (numTrace)

The following are file features extracted in this way:

  - file size (sizeTrace)
  - location distribution (locationTrace)
