package shop.inventa.pg2sns4k.helper

import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(MockKExtension::class)
class JsonHelperTest {

    @Test
    fun `Json string to JSONObject`() {
        val jsonString = """
            {
                "name": "John",
                "age": 30,
                "car": null
            }
        """.trimIndent()

        val jsonObject = JsonHelper.fromJsonString(jsonString)

        assertEquals("John", jsonObject.getString("name"))
        assertEquals(30, jsonObject.getInt("age"))
        assertTrue(jsonObject.isNull("car"))
    }

    @Test
    fun `JSONObject to Json string`() {
        val jsonString = """{"car":null,"name":"John","age":30}"""

        val jsonObject = JsonHelper.fromJsonString(jsonString)

        val newJsonString = JsonHelper.toJsonString(jsonObject)

        assertEquals(jsonString, newJsonString)
    }
}
