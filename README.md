# spark-vcf

Spark VCF data source implementation in native spark.

## Introduction

Spark VCF allows you to natively load VCFs into an Apache Spark Dataframe/Dataset. To get started with Spark-VCF, you can 
clone or download this repository, then run `mvn package` and use the jar. We are also now in Maven central.

Since spark-vcf is written specifically for Spark, there is less overhead and performance gains in many areas.

## Installation

Spark-vcf can be packaged from source or added as a dependency to your Maven based project.

To install spark vcf, add the following to your pom:

```xml
<dependency>
  <groupId>com.lifeomic</groupId>
  <artifactId>spark-vcf</artifactId>
  <version>0.3.0</version>
</dependency>
```

For sbt:
```
libraryDependencies += "com.lifeomic" % "spark-vcf" % "0.3.0"
```

If you are using gradle, the dependency is:
```
compile group: 'com.lifeomic', name: 'spark-vcf', version: '0.3.0'
```

## Getting Started

Getting started with Spark VCF is as simple as:

```scala
val myVcf = spark.read
    .format("com.lifeomic.variants")
    .load("src/test/resources/example.vcf")
```

The schema contains the standard vcf columns and has the options to expand INFO and/or FORMAT columns. An example schema 
from 1000 genomes is shown below:

```
 |-- chrom: string (nullable = true)
 |-- pos: long (nullable = true)
 |-- start: long (nullable = true)
 |-- stop: long (nullable = true)
 |-- id: string (nullable = true)
 |-- ref: string (nullable = true)
 |-- alt: string (nullable = true)
 |-- qual: string (nullable = true)
 |-- filter: string (nullable = true)
 |-- info: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
 |-- gt: string (nullable = true)
 |-- sampleid: string (nullable = true)

```

There are options that you can use as well for the `Format` and `Info` columns. To return the format fields as a map, 
instead of separate fields, you can set the `use.format.map` variable to `true`. This can be used to speed up the spark 
job even more, as it doesn't have to read the header file for type and column information.

```scala
val mappedFormat = spark.read
    .format("com.lifeomic.variants")
    .option("use.format.map", "true")
    .load("src/test/resources/example.vcf")
```

You can also stringly type the formats as well by setting `use.format.type` to false.

One more note worth mentioning: while the core of spark-vcf is written as a Spark data source, it is still advisable to use 
the BGZFEnhancedGzipCodec from Hadoop-BAM for splitting bgzip files, so that Spark can properly partition the files. For example:

```scala
val sparkConf = new SparkConf()
    .setAppName("testing")
    .setMaster("local[8]")
    .set("spark.hadoop.io.compression.codecs", "org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec")
```

## TODO

* Provide performance benchmarks compared to other libraries

## License

[The MIT License](LICENSE)
