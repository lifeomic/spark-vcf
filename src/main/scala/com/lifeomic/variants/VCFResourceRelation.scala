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


class VCFResourceRelation(
                             override val sqlContext: SQLContext,
                             path: String,
                             useFormatTypes: Boolean = true,
                             useFormatAsMap: Boolean = false,
                             useAnnotationTypes: Boolean = false
                         )
    extends BaseRelation with Serializable with TableScan {

    private val vcf = sqlContext.sparkContext
        .textFile(path)
        .cache()

    private val quickLoad = vcf.filter(item => !item.startsWith("##")).cache()
    private val headers = quickLoad.filter(item => item.startsWith("#")).first().split("\t")
    private val values = quickLoad.filter(item => !item.startsWith("#"))

    private val metaHandler = (item: String) => {
        val z = item.replace("<", "").replace("##FORMAT=", "")
        val filtered = z.split(",").filter(item => item.startsWith("ID") || item.startsWith("Type") || item.startsWith("Number"))
        var key = ""
        var value = ""
        var number = ""
        for (f <- filtered) {
            val spl = f.split("=")
            if (spl(0).equals("ID")){
                key = spl(1)
            } else if(spl(0).equals("Type")) {
                value = spl(1)
            } else if (spl(0).equals("Number")) {
                number = spl(1)
            }
        }
        if (value.equals("")) {
            value = "String"
        }
        (key, (value, number))
    }

    private val structHandler = (item: (String, (String, String))) => {
        val (key, value) = item
        val (v, number) = value
        val dType = v match {
            case "Float" => if (number != "1") ArrayType(FloatType) else FloatType
            case "Integer" => if (number != "1") ArrayType(IntegerType) else IntegerType
            case _ => StringType
        }
        StructField(key.toLowerCase(), dType)
    }

    private val formats = vcf.filter(item => item.startsWith("##FORMAT")).map(metaHandler)
    private val annotations = vcf.filter(item => item.startsWith("##INFO")).map(metaHandler)


    /**
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
      * 9. info
      * 10. Format fields
      * last sampleid
      */
    override val schema: StructType = inferSchema()

    private def inferSchema() : StructType = {

        val ranger = List.range(0, if (headers.length > 10) 10 else headers.length)
        val structFields = ranger.flatMap(i => {
            if (i == 1) {
                Seq(
                    StructField(headers(i).toLowerCase, LongType),
                    StructField("start", LongType),
                    StructField("stop", LongType)
                )
            }
            else if (i == 7){
                Seq(StructField(headers(i).toLowerCase, MapType(StringType, StringType)))
            }
            else if (i == 8) {
                if (useFormatAsMap) {
                    //treat format as a map - faster
                    Seq(StructField("format", MapType(StringType, StringType)))
                } else if (useFormatTypes) {
                    val collectedFormats = formats.collectAsMap()
                    collectedFormats.map(structHandler)
                }  else {
                    //use stringly typed
                    val first = values.first().split("\t")(8)
                    val joined = first.split(":")
                    joined.map(item => {
                        StructField(item.toLowerCase(), StringType)
                    })
                }
            }
            else if (i == 9){
                Seq(StructField("sampleid", StringType))
            }
            else {
                Seq(StructField(headers(i).replace("#", "").toLowerCase, StringType))
            }
        })
        StructType(structFields)
    }

    override def buildScan(): RDD[Row] = {
        val schFields = schema.fields.map(item => (item.name, item.dataType.typeName, item.dataType.sql.toLowerCase))
        values.filter(item => !item.startsWith("#"))
            .flatMap(row => {
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
                            if (keyValue.length < 2) {
                                null
                            } else {
                                (keyValue(0), keyValue(1))
                            }
                        }).filter(_ != null).toMap
                    }

                    if (split.length > 9) {
                        val format = split(8)
                        val splitFormat = format.split(":")
                        val altSplit = alternate.split(",")
                        val formatExtendedFields = schFields.slice(10, schFields.length - 1)
                        altSplit.flatMap(altS => {
                            List.range(9, split.length).map(i => {
                                val sampleValues = split(i).split(":")
                                val rangeValues = List.range(0, splitFormat.length)
                                val formatMap = rangeValues.map(v => (splitFormat(v).toLowerCase(), sampleValues(v))).toMap
                                val extend = if (useFormatAsMap) {
                                    Array(formatMap)
                                } else {
                                    formatExtendedFields.map(item => {
                                        val (key, value, sq) = item
                                        sq match {
                                            case "int" => formatMap.get(key).map(_.toInt)
                                            case "array<int>" => formatMap.get(key).map(item => item.split(",").map(_.toInt)).orNull
                                            case "float" => formatMap.getOrElse(key, null).toDouble
                                            case "array<float>" => formatMap.get(key).map(item => item.split(",").map(_.toFloat)).orNull
                                            case _ => formatMap.getOrElse(key, null)
                                        }
                                    })
                                }
                                val updateFields = Array(chromosome, position, startpoint, endpoint, id, reference, altS, qual, filter, annotations)
                                Row.fromSeq(updateFields ++ extend ++ Array(headers(i)))
                            })
                        }).toSeq
                    } else {
                        Seq(Row.fromSeq(initialFields ++ Array(annotations)))
                    }
                } else {
                    Seq(Row.fromSeq(initialFields))
                }
            })
    }

}

