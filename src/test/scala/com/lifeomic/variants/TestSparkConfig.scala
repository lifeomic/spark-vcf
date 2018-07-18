package com.lifeomic.variants

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestSparkConfig extends LazyLogging {

    private val PROPERTY_LOGGING_LEVEL: String = "com.lifeomic.variants.testing.logging.level"

    val sparkConf: SparkConf =
        new SparkConf()
            .setAppName("testing")
            .setMaster("local[8]")
            .set("spark.hadoop.io.compression.codecs", "org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec")

    val spark: SparkSession =
        SparkSession.builder()
            .config(sparkConf)
            .getOrCreate()

    // Set spark logging level
    private val level = System.getProperty(PROPERTY_LOGGING_LEVEL, "ERROR")
    logger.info(s"Setting spark logging level to $level. You can change this by setting the property '$PROPERTY_LOGGING_LEVEL'...")
    spark.sparkContext.setLogLevel(level)

}
