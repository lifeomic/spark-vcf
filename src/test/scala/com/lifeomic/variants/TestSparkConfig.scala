package com.lifeomic.variants

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestSparkConfig {

    val sparkConf = new SparkConf().setAppName("testing").setMaster("local[8]")
        .set("spark.hadoop.io.compression.codecs", "org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
}
