package br.com.fltech.outbox.publisher.helper

import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(MockKExtension::class)
class JsonHelperTest {

    @Test
    fun `Json string to JSONObject`() {
        // Real-world Shopify product payload — kept in a resource file
        // so this test stays under detekt's MaxLineLength without an
        // inline `@Suppress`. Edit the fixture in
        // `src/test/resources/fixtures/shopify-product.json` when the
        // upstream shape changes.
        val jsonString = loadFixture("/fixtures/shopify-product.json")

        val jsonObject = JsonHelper.fromJsonString(jsonString)

        assertEquals(7819670847641, jsonObject.getLong("id"))
        assertEquals("Água Aromática Four Elements Sinergia Receber 200ml", jsonObject.getString("name"))
    }

    @Test
    fun `JSONObject to Json string`() {
        val jsonString = """{"car":null,"name":"John","age":30}"""

        val jsonObject = JsonHelper.fromJsonString(jsonString)

        val newJsonString = JsonHelper.toJsonString(jsonObject)

        assertEquals(jsonString, newJsonString)
    }

    private fun loadFixture(path: String): String =
        checkNotNull(this::class.java.getResource(path)) { "fixture not found: $path" }
            .readText(Charsets.UTF_8)
            .trimEnd()
}
