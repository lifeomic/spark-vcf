package com.lifeomic.variants

import org.junit.{Assert, Test}
import org.scalatest.junit.AssertionsForJUnit
import org.apache.spark.sql.functions._

import scala.collection.mutable

class VCFDatasourceTest extends AssertionsForJUnit {

    private val spark = TestSparkConfig.spark

    @Test
    def testGeneralData() : Unit = {
        val y = spark.read
            .format("com.lifeomic.variants")
            .load("src/test/resources/example.vcf")
        val x = y.collect()
        Assert.assertEquals(x.length, 668)
        val first = x.head
        val chrom = first.getAs[String]("chrom")
        Assert.assertEquals(chrom, "chr1")
        Assert.assertEquals(first.getAs[Long]("pos"), 13273)
        Assert.assertEquals(first.getAs[String]("id"), "rs531730856")
        val annotations = first.getAs[Map[String, String]]("info")
        Assert.assertEquals(annotations.size, 28)
        Assert.assertEquals(annotations("dbSNP_CAF"), "0.905,0.09505")
        Assert.assertEquals(first.getAs[mutable.WrappedArray[Integer]]("ad").length, 2)
        Assert.assertEquals(first.getAs[mutable.WrappedArray[Integer]]("sac").length, 4)
        Assert.assertEquals(first.getAs[Integer]("dp"), 20)
        Assert.assertEquals(y.schema.fields.length, 17)
    }

    @Test
    def test1000Genomes() : Unit = {
        val y = spark.read
            .format("com.lifeomic.variants")
            .load("src/test/resources/chr2.vcf")
        val count = y.count()
        val first = y.first()
        Assert.assertEquals(first.getAs[String]("sampleid"), "HG00096")
        Assert.assertEquals(first.getAs[String]("ref"), "TA")
        Assert.assertEquals(count, 307992)
        Assert.assertEquals(first.getAs[Map[String, String]]("info").size, 11)
        Assert.assertEquals(first.getAs[String]("gt"), "0|0")
        Assert.assertEquals(y.schema.fields.length, 12)
    }

    @Test
    def testStringlyTyped() : Unit = {
        val y = spark.read
            .format("com.lifeomic.variants")
            .option("use.format.type", "false")
            .load("src/test/resources/example.vcf")
        val x = y.collect()
        val first = x.head
        Assert.assertEquals(first.getAs[String]("ad"), "10,10")
        Assert.assertEquals(first.getAs[String]("sac"), "3,7,4,6")
        Assert.assertEquals(first.getAs[String]("dp"), "20")
    }

    @Test
    def testMapStyle() : Unit = {
        val y = spark.read
            .format("com.lifeomic.variants")
            .option("use.format.map", "true")
            .load("src/test/resources/example.vcf")
        val x = y.collect()
        val first = x.head
        Assert.assertEquals(first.getAs[Map[String,String]]("format").size, 6)
        Assert.assertEquals(first.getAs[Map[String,String]]("format")("gt"), "0/1")
    }

}
