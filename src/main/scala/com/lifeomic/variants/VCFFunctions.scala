package com.lifeomic.variants

import com.lifeomic.variants.VCFConstants._

object VCFFunctions {

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

    def fieldsExtended(mapFlag: Boolean,
                       map: Map[String, String],
                       schFields: Array[(String, String, String)],
                       start: Int, end: Int) : Array[_ >: Map[String, String]] = {
        if (mapFlag) {
            Array(map)
        } else {
            schFields.slice(start, end)
                .map(item => {
                    val (key, value, sq) = item
                    sq match {
                        case "int" => map.get(key).map(_.toInt).getOrElse(null)
                        case "array<int>" => map.get(key).map(item => item.split(",").map(_.toInt)).getOrElse(null)
                        case "float" => map.get(key).map(_.toFloat).getOrElse(null)
                        case "array<float>" => map.get(key).map(item => item.split(",").map(_.toFloat)).getOrElse(null)
                        case _ => map.getOrElse(key, null)
                    }
                })
        }
    }

}
