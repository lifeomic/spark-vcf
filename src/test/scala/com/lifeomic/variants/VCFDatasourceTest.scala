package com.lifeomic.variants

import org.junit.{Assert, Test}
import org.scalatest.junit.AssertionsForJUnit

import scala.collection.mutable

class VCFDatasourceTest extends AssertionsForJUnit {

    private val spark = TestSparkConfig.spark
    import spark.implicits._

    @Test
    def testGeneralData() : Unit = {
        val y = spark.read
            .format("com.lifeomic.variants")
            .load("src/test/resources/correct/example.vcf")
        val x = y.collect()
        Assert.assertEquals(x.length, 668)
        val first = x.head
        val chrom = first.getAs[String]("chrom")
        Assert.assertEquals(chrom, "chr1")
        Assert.assertEquals(first.getAs[Long]("pos"), 13273)
        Assert.assertEquals(first.getAs[String]("id"), "rs531730856")
        val annotations = first.getAs[Map[String, String]]("info")
        Assert.assertEquals(annotations.size, 28)
        Assert.assertEquals(annotations("dbsnp_caf"), "0.905,0.09505")
        Assert.assertEquals(first.getAs[mutable.WrappedArray[Integer]]("ad").length, 2)
        Assert.assertEquals(first.getAs[mutable.WrappedArray[Integer]]("sac").length, 4)
        Assert.assertEquals(first.getAs[Integer]("dp"), 20)
        Assert.assertEquals(y.schema.fields.length, 17)
    }

    @Test
    def test1000Genomes() : Unit = {
        val y = spark.read
            .format("com.lifeomic.variants")
            .load("src/test/resources/correct/chr2.vcf")
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
            .load("src/test/resources/correct/example.vcf")
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
            .load("src/test/resources/correct/example.vcf")
        val x = y.collect()
        val first = x.head
        Assert.assertEquals(first.getAs[Map[String,String]]("format").size, 6)
        Assert.assertEquals(first.getAs[Map[String,String]]("format")("gt"), "0/1")
    }

    @Test
    def testMultipleDifferentFiles() : Unit = {
        val y = spark.read
            .format("com.lifeomic.variants")
            .option("use.format.map", "true")
            .load("src/test/resources/correct/*.vcf")
        val example = y.filter(item => item.getAs[Map[String, String]]("format").size > 2).first()
        val thousandGenomes = y.filter(item => item.getAs[Map[String, String]]("format").size < 2).first()
        Assert.assertEquals(example.getString(0), "chr1")
        Assert.assertEquals(thousandGenomes.getString(0), "2")
        Assert.assertEquals(thousandGenomes.getString(11), "HG00096")
        Assert.assertEquals(example.get(11), "NA01531")
    }

    @Test
    def testAnnotationsType() : Unit = {
        val y = spark.read
            .format("com.lifeomic.variants")
            .option("use.info.type", "true")
            .load("src/test/resources/correct/chr2.vcf")
        val x = y.collect()
        val first = x.head
        Assert.assertEquals(first.getAs[String]("ssid"), "ss1368084858")
        Assert.assertEquals(first.getAs[Integer]("ns"), 2504)
        Assert.assertEquals(first.getAs[String]("gt"), "0|0")
    }

}
