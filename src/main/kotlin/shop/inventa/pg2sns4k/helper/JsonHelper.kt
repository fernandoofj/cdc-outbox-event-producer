package shop.inventa.pg2sns4k.helper

import org.json.JSONObject

object JsonHelper {

    fun fromJsonString(json: String) = JSONObject(json)

    fun toJsonString(jsonObject: JSONObject) = jsonObject.toString()
}
