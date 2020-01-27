package com.lifeomic.variants

import com.lifeomic.variants.VCFConstants._

import scala.util.{Failure, Success, Try}

object VCFFunctions {

    /**
      * Returns a meta row of the key, value and number
      * @param t formatType
      * @return
      */
    def metaHandler(t: String) : (String) => (String, (String, String)) = (item: String) => {
        val z = item.replace("<", "").replace(t, "")
        val filtered = z.split(",").filter(item => item.startsWith(ID) || item.startsWith(TYPE) || item.startsWith(NUMBER))
        var key = ""
        var value = ""
        var number = ""
        for (f <- filtered) {
            val spl = f.split("=")
            if (spl(0).equalsIgnoreCase(ID)){
                key = spl(1)
            } else if(spl(0).equalsIgnoreCase(TYPE)) {
                value = spl(1)
            } else if (spl(0).equalsIgnoreCase(NUMBER)) {
                number = spl(1)
            }
        }
        if (value.equals("")) {
            value = "String"
        }
        (key, (value, number))
    }

    def conversionHandler[T](x: String, defaultValue: T, conversionFunc: String => T): T = {
        Try(conversionFunc(x)) match {
            case Success(s) => s
            case Failure(_) => defaultValue
        }
    }

    /**
      * Extends fields for format and info columns
      * @param mapFlag should use map or not
      * @param map parameter map
      * @param schFields column fields
      * @param start start index
      * @param end end index
      * @return
      */
    def fieldsExtended(mapFlag: Boolean,
                       map: Map[String, String],
                       schFields: Array[(String, String, String)],
                       start: Int, end: Int) : Array[_ >: Map[String, String]] = {
        if (mapFlag) {
            Array(map)
        } else {
            val integerHandler = (x: String) => {
                if (x != null && !x.equals(".") && x.forall(_.isDigit)) x.toInt
                else if (x != null && !x.equals(".") && !x.isEmpty) conversionHandler(x, 0, x => java.lang.Double.valueOf(x).intValue())
                else 0
            }
            val floatHandler = (x: String) => {
                val tmp = x.replace(".", "")
                if (x != null && !x.equals(".") && tmp.forall(_.isDigit)) x.toFloat
                else if (x != null && !x.equals(".") && !x.isEmpty) conversionHandler(x, 0f, x => java.lang.Double.valueOf(x).floatValue())
                else 0f
            }

            val z = schFields.slice(start, end)
                .map(item => {
                    val (k, value, sq) = item
                    val key = k.replace("info_", "").replace("format_", "")

                    sq match {
                        case "int" => map.get(key)
                            .map(integerHandler)
                            .getOrElse(null)
                        case "array<int>" => map.get(key)
                            .map(item => item.split(",")
                                .map(integerHandler))
                            .getOrElse(null)
                        case "float" => map.get(key)
                            .map(floatHandler)
                            .getOrElse(null)
                        case "array<float>" => map.get(key)
                            .map(item => item.split(",")
                                .map(floatHandler))
                            .getOrElse(null)
                        case _ => map.getOrElse(key, null)
                    }
                })
            z
        }
    }

}
