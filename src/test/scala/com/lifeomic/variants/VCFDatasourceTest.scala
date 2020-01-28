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
        Assert.assertEquals(annotations.size, 36)
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
        Assert.assertEquals(first.getAs[Map[String, String]]("info").size, 13)
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
        Assert.assertEquals(first.getAs[String]("info_ssid"), "ss1368084858")
        Assert.assertEquals(first.getAs[Integer]("info_ns"), 2504)
        Assert.assertEquals(first.getAs[String]("gt"), "0|0")
    }

    @Test
    def testDecimals(): Unit = {
        val y = spark.read
            .format("com.lifeomic.variants")
            .option("use.info.type", "true")
            .load("src/test/resources/small.vcf")
        val first = y.first()
        Assert.assertEquals(first.getString(0), "chr1")
        Assert.assertEquals(first.getLong(1), 3348577)
        Assert.assertEquals(first.getString(5), "C")
        Assert.assertEquals(first.getString(6), "T")
    }

    @Test
    def verifyItWorksInfoTypes() : Unit = {
        val y = spark.read
            .format("com.lifeomic.variants")
            .option("use.info.type", "true")
            .load("src/test/resources/issue2.vcf")
        val cc = y.collect()
        val first = cc.head
        Assert.assertEquals(first.getAs[mutable.WrappedArray[Integer]]("info_ac")(0), 86)
        Assert.assertTrue(first.getAs[mutable.WrappedArray[Float]]("info_af")(0) == 0.4886f)
        Assert.assertEquals(first.getAs[Integer]("info_an"), 176)
        Assert.assertEquals(first.getAs[String]("info_db"), "DB")
        Assert.assertEquals(first.getAs[Integer]("info_dp"), 17307)
        Assert.assertEquals(first.getAs[String]("info_maj"), "REF")
    }

    @Test
    def testWebsiteChange() : Unit = {
        val y = spark.read
            .format("com.lifeomic.variants")
            .option("use.info.type", "true")
            .load("src/test/resources/site.vcf")
        val cc = y.collect()
        val first = cc.head
        Assert.assertEquals(first.getAs[String]("chrom"), "20")
        Assert.assertEquals(first.getAs[Long]("pos"), 14370L)
        Assert.assertEquals(first.getAs[String]("ref"), "G")
        Assert.assertEquals(first.getAs[String]("alt"), "A")
        Assert.assertEquals(first.getAs[String]("info_aa"), null)
        Assert.assertEquals(first.getAs[Integer]("info_dp"), 14)
        Assert.assertEquals(first.getAs[Integer]("info_ns"), 3)
        Assert.assertTrue(first.getAs[mutable.WrappedArray[Float]]("info_af")(0) == 0.5f)
        Assert.assertEquals(first.getAs[String]("info_h2"), "H2")
        Assert.assertEquals(first.getAs[String]("info_db"), "DB")
        Assert.assertEquals(first.getAs[Int]("dp"), 1)
        Assert.assertEquals(first.getAs[mutable.WrappedArray[Int]]("hq")(0), 51)
        Assert.assertEquals(first.getAs[Int]("gq"), 48)
        Assert.assertEquals(first.getAs[String]("gt"), "0|0")
        Assert.assertEquals(first.getAs[String]("sampleid"), "NA00001")

        val second = cc(1)
        Assert.assertEquals(second.getAs[String]("sampleid"), "NA00002")

        val third = cc(2)
        Assert.assertEquals(third.getAs[String]("sampleid"), "NA00003")
    }

    @Test
    def testInvalidNumberFormat(): Unit = {
        val y = spark.read
            .format("com.lifeomic.variants")
            .option("use.info.type", "true")
            .load("src/test/resources/invalid-number-format.vcf")

        val first = y.first()
        Assert.assertTrue(first.getAs[mutable.WrappedArray[Float]]("info_dbnsfp_phastcons100way_vertebrate_rankscore")(0) == 0.0f)
        Assert.assertTrue(first.getAs[mutable.WrappedArray[Int]]("info_dbnsfp_exac_sas_ac")(0) == 0)
        Assert.assertEquals(0, first.getAs[Int]("info_dbsnpbuildid"))
    }

}
