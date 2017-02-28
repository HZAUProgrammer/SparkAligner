# SparkAligner

SparkAligner is generalized version of [SparkBWA](https://github.com/citiususc/SparkBWA) with
support for modules. These modules can be used to add support for other aligners.

The following aligners are currently supported:
* [bwa](http://bio-bwa.sourceforge.net/bwa.shtml)

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
-R /data/hg19/hg19.fasta                     \
-partitions 2                                \
-I /data/input/datasets
```

#### Running the provided Docker example
The docker image can be built using
```
docker build --no-cache -t <name of Docker image> .
```

It can also be found
[here](https://hub.docker.com/r/paalka/spark-aligner/).
This docker image also downloads a test dataset (the Lambda phage dataset)
from [f.128.no](http://f.128.no/genomics/test_data/)).

```
docker run                                                                   \
-it                                                                          \
-v <path to data folder>:<path to mount the data folder inside to container> \
paalka/spark-aligner
<regular spark-aligner arguments here>
```

In order to keep the SAM files, you need to mount the data directory to the
container, ex:
```
docker run -it                             \
-v <path to test data>:/test_data          \
paalka/spark-aligner bwa                   \
-algorithm mem                             \
-R /data/reference/lambda_virus.fa         \
-I /test_data/<test_data_folder>           \
-partitions 2 -bwaArgs "-t 4"
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
