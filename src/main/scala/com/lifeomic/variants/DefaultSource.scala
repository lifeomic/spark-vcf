package com.lifeomic.variants

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider with SchemaRelationProvider {

    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
        createRelation(sqlContext, parameters, null)
    }

    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
        createPrivateRelation(sqlContext, parameters)
    }

    private def createPrivateRelation(sqlContext: SQLContext, parameters: Map[String, String]) : BaseRelation = {
        val path = parameters.get("path")

        val useFormatMap = parameters.get("use.format.map").map(_.toBoolean).getOrElse(false)
        val useFormatTypes = parameters.get("use.format.type").map(_.toBoolean).getOrElse(true)

        path match {
            case Some(p) => new VCFResourceRelation(sqlContext, p, useFormatTypes=useFormatTypes, useFormatAsMap=useFormatMap)
            case _ => throw new IllegalArgumentException("Path is required")
        }
    }
}

