package br.com.fltech.cdc.outbox.publisher.helper

import org.json.JSONObject

object JsonHelper {

    fun fromJsonString(json: String) = JSONObject(json)

    fun toJsonString(jsonObject: JSONObject) = jsonObject.toString()
}
