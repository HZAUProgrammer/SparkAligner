# SparkAligner

SparkAligner is generalized version of [SparkBWA](https://github.com/citiususc/SparkBWA) with
support for modules. These modules can be used to add support for other aligners.

The following aligners are currently supported:
* bwa

### Usage
```
spark-submit                                 \
--class com.github.sparkaligner.SparkAligner \
sparkaligner.jar                             \
<name of aligner to use>                     \
[<aligner specific options>]                 
```

Ex.
```
spark-submit                                 \
--class com.github.sparkaligner.SparkAligner \
sparkaligner.jar                             \
bwa                                          \
-algorithm mem                               \
-index /data/hg19/hg19.fasta                 \
-partitions 2                                \
/data/input/datasets                         
```

### Building
Make sure to clone the project using `git clone --recursive`, as it uses
submodules.
The JAR can be built by using `make`.

### Adding new modules
The folder `aligners` contains the code for each module. New aligners are
required to extends the abstract class `BaseAligner`, which performs most of
the Spark related work. This means that in order to add a new aligner, you
only need to specify how to process the arguments for the aligner, and manage
how the aligner will be run.
