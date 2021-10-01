# GDSim Trace Generator

This trace generator was designed to expand build new traces from existing traces.

## How to use

Run `generator` on a directory with the `.gen` and `.filegen` files that contain the distributions that you want to use in the synthetic generation.
Currently the empirical location distribution is not used due to the lack of location information in the traces we have used.
Instead, the generator will use a Zipf distribution.
You can configure the skew of the Zipf distribution with the `-skew` option.
You can also use the `-total` option to define how many jobs to create.
The `-jobName` and `-fileName` options define the name of the files with job and data information.
The `-seed` option defines the random seed to be used. By default it is zero, so that you can reproduce datasets that you create in this. The seed should be a 64 bits integer.
