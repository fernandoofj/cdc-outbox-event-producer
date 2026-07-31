package br.com.fltech.outbox.publisher.helper

import org.json.JSONObject

internal object JsonHelper {
    fun fromJsonString(json: String) = JSONObject(json)

    fun toJsonString(jsonObject: JSONObject) = jsonObject.toString()
}
