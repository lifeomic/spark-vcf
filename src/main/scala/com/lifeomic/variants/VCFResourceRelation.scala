/**
  * The MIT License
  *
  * Copyright 2017 Lifeomic
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in
  * all copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  * THE SOFTWARE.
  */

package com.lifeomic.variants


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.lifeomic.variants.VCFConstants._

/**
  * Spark vcf resource relation
  * @param sqlContext Spark sql context
  * @param path path of vcf file(s)
  * @param useFormatTypes Type checking for formats, plus casting types
  * @param useFormatAsMap Use the format column as a map
  * @param useAnnotationTypes Type casting for info fields
  * @param useAnnotationAsMap use annotations as a map
  */
class VCFResourceRelation(
                             override val sqlContext: SQLContext,
                             path: String,
                             useFormatTypes: Boolean = true,
                             useFormatAsMap: Boolean = false,
                             useAnnotationTypes: Boolean = false,
                             useAnnotationAsMap: Boolean = true
                         )
    extends BaseRelation with Serializable with TableScan {

    import sqlContext.sparkSession.implicits._

    private val vcf = sqlContext.sparkSession
        .read
        .text(path)
        .select(input_file_name.as(FILE_NAME), col(VALUE).as(TEXT_VALUE))

    private val quickLoad = vcf.filter(!col(TEXT_VALUE).startsWith("##"))

    private val headerMap = quickLoad.filter(col(TEXT_VALUE).startsWith("#"))
        .map(item => (item.getString(0), item.getString(1).split("\t")))
        .collect()
        .toMap

    private val headerMapBroadcast = sqlContext.sparkContext.broadcast(headerMap)

    private val values = quickLoad.filter(!col(TEXT_VALUE).startsWith("#")).rdd.map(item => (item.getString(0), item.getString(1)))

    private val structHandler = (prefix: String) => {
        (item: (String, (String, String))) => {
            val (key, value) = item
            val (v, number) = value
            val condition = number != null && (number.equals("0") || number.equals("1"))
            val dType = v match {
                case "Float" => if (condition) FloatType else ArrayType(FloatType)
                case "Integer" => if (condition) IntegerType else ArrayType(IntegerType)
                case _ => StringType
            }
            StructField(prefix + key.toLowerCase(), dType)

        }
    }

    private val formats = vcf.filter(col(TEXT_VALUE).startsWith("##FORMAT")).map(_.getString(1)).rdd.map(VCFFunctions.metaHandler("##FORMAT="))
    private val annotations = vcf.filter(col(TEXT_VALUE).startsWith("##INFO")).map(_.getString(1)).rdd.map(VCFFunctions.metaHandler("##INFO="))
    private var annotationCount = 1

    /*
      * order is
      * 0. chromosome
      * 1. position
      * 2. start
      * 3. stop
      * 4. id
      * 5. reference
      * 6. alternate
      * 7. quality
      * 8. filter
      * 9. info fields
      * 10. Format fields
      * last sampleid
      */
    override val schema: StructType = inferSchema()

    /**
      * Runs the vcf queries and converts them to an rdd of rows
      * @return rdd of a spark sql row
      */
    override def buildScan(): RDD[Row] = {
        val schFields = schema.fields.map(item => (item.name, item.dataType.typeName, item.dataType.sql.toLowerCase))
        val annotateCount = annotationCount
        values.filter(!_._2.startsWith("#"))
            .flatMap {
                case (fname, row) => {
                    val split = row.split("\t")
                    val chromosome = split(0)
                    val position = split(1).toLong
                    val id = split(2)
                    val reference = split(3)
                    val alternate = split(4)
                    val qual = split(5)
                    val filter = split(6)
                    val startpoint = position - 1
                    val endpoint = startpoint + reference.length

                    val initialFields = Array(chromosome, position, startpoint, endpoint, id, reference, alternate, qual, filter)

                    if (split.length > 7) {
                        val annotations = {
                            val splitCols = split(7).split(";")
                            splitCols.map(item => {
                                val keyValue = item.split("=")
                                if (keyValue.length < 2 && keyValue.length > 0) {
                                    (keyValue(0).toLowerCase, keyValue(0))
                                } else if (keyValue.length < 1) {
                                    null
                                } else {
                                    (keyValue(0).toLowerCase, keyValue(1))
                                }
                            }).filter(_ != null).toMap
                        }

                        val annotationsExtended = VCFFunctions.fieldsExtended(
                            useAnnotationAsMap, annotations, schFields, 9, 9 + annotateCount
                        )

                        if (split.length > 9) {
                            val format = split(8)
                            val splitFormat = format.split(":")
                            val altSplit = alternate.split(",")
                            val mapper = headerMapBroadcast.value
                            altSplit.flatMap(altS => {
                                List.range(9, split.length).map(i => {
                                    val sampleValues = {
                                        val initial = split(i).split(":")
                                        if (initial.length < splitFormat.length){
                                            val diff = splitFormat.length - initial.length
                                            initial ++ Array.fill(diff)(".")
                                        } else initial
                                    }
                                    val rangeValues = List.range(0, splitFormat.length)
                                    val formatMap = rangeValues.map(v => (splitFormat(v).toLowerCase(), sampleValues(v))).toMap
                                    val extend = VCFFunctions.fieldsExtended(
                                        useFormatAsMap, formatMap, schFields, 9+ annotateCount,schFields.length - 1
                                    )
                                    val updateFields = Array(chromosome, position, startpoint, endpoint, id, reference, altS, qual, filter)
                                    Row.fromSeq(updateFields ++ annotationsExtended ++ extend ++ Array(mapper(fname)(i)))
                                })
                            }).toSeq
                        } else {
                            Seq(Row.fromSeq(initialFields ++ annotationsExtended))
                        }
                    } else {
                        Seq(Row.fromSeq(initialFields))
                    }
                }
            }
    }

    private def validateHeaders() : Unit = {
        var currentHeaderCount = -1
        if (headerMap == null || headerMap.isEmpty) {
            throw new IllegalArgumentException("VCF file(s) does not have a valid header")
        }
        for ((key, value) <- headerMap) {
            val split = value.slice(0, 10)
            if (currentHeaderCount == -1) {
                currentHeaderCount = split.length
            } else if (currentHeaderCount != split.length) {
                throw new IllegalArgumentException("Currently cannot pass multiple vcf files with different sized main columns (outside of samples)")
            }
        }
    }

    private def formatOptionFields(columnName: String, rdd : RDD[(String, (String, String))],
                                   useMap: Boolean, useTypes: Boolean, prefix: String = "") : Seq[StructField] = {
        if (useMap) {
            Seq(StructField(columnName.toLowerCase, MapType(StringType, StringType)))
        } else if (useTypes) {
            val collectedFormats = rdd.collectAsMap()
            collectedFormats.map(structHandler(prefix)).toSeq
        } else {
            val first = values.first()._2.split("\t")(8)
            val joined = first.split(":")
            joined.map(item => {
                StructField(item.toLowerCase(), StringType)
            })
        }
    }

    private def inferSchema() : StructType = {
        validateHeaders()

        val firstHeader = headerMap.head._2
        val ranger = List.range(0, if (firstHeader.length >= 10) 10 else firstHeader.length)
        val structFields = ranger.flatMap(i => {
            if (i == 1) {
                Seq (
                    StructField(firstHeader(i).toLowerCase, LongType),
                    StructField("start", LongType),
                    StructField("stop", LongType)
                )
            }
            else if (i == 7){
                val toRet = formatOptionFields(
                    firstHeader(i).toLowerCase, annotations, useAnnotationAsMap, useAnnotationTypes, "info_"
                )
                annotationCount = toRet.size
                toRet
            }
            else if (i == 8) {
                formatOptionFields(firstHeader(i).toLowerCase, formats, useFormatAsMap, useFormatTypes, "")
            }
            else if (i == 9){
                Seq(StructField("sampleid", StringType))
            }
            else {
                Seq(StructField(firstHeader(i).replace("#", "").toLowerCase, StringType))
            }
        })
        StructType(structFields)
    }

}
