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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider with SchemaRelationProvider {

    /**
      * Creates relation
      * @param sqlContext spark sql context
      * @param parameters parameters for job
      * @return Base relation
      */
    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
        createRelation(sqlContext, parameters, null)
    }

    /**
      * Creates relation with user schema
      * @param sqlContext spark sql context
      * @param parameters parameters for job
      * @param schema user defined schema
      * @return Base relation
      */
    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
        createPrivateRelation(sqlContext, parameters)
    }

    private def createPrivateRelation(sqlContext: SQLContext, parameters: Map[String, String]) : BaseRelation = {
        val path = parameters.get("path")

        val useFormatMap = parameters.get("use.format.map").map(_.toBoolean).getOrElse(false)
        val useFormatTypes = parameters.get("use.format.type").map(_.toBoolean).getOrElse(true)
        val useAnnotationTypes = parameters.get("use.info.type").map(_.toBoolean).getOrElse(false)
        val useAnnotationAsMap = if (useAnnotationTypes) false else parameters.get("use.info.map").map(_.toBoolean).getOrElse(true)

        path match {
            case Some(p) => new VCFResourceRelation(
                sqlContext, p,
                useFormatTypes=useFormatTypes,
                useFormatAsMap=useFormatMap,
                useAnnotationAsMap=useAnnotationAsMap,
                useAnnotationTypes= useAnnotationTypes
            )
            case _ => throw new IllegalArgumentException("Path is required")
        }
    }
}

