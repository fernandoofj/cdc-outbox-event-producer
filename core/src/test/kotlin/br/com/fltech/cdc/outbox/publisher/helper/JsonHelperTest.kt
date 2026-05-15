package br.com.fltech.cdc.outbox.publisher.helper

import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

@Suppress("MaxLineLength")
@ExtendWith(MockKExtension::class)
class JsonHelperTest {

    @Test
    fun `Json string to JSONObject`() {
        val jsonString = """{"id":7819670847641,"name":"Água Aromática Four Elements Sinergia Receber 200ml","description":"...","slug":"alchi-agua-aromatica-four-elements-sinergia-receber-200-ml","vendorTradeName":"Alchi","status":"ACTIVE","tags":"Categoria1_Beleza e Bem-Estar, Categoria2_Aromaterapia, Categoria3_Águas Aromáticas, Cruelty Free, EAN_7898329484051, Exclusivas_, Fabricante_Alchi, Fornecedor_Alchi, FP_Sync, Marca_Alchi, NCM_33074900, new_mktplace_fifth_sync, new_mktplace_sync, Origem_Rio Grande do Sul, Pedido mínimo (marca)_900, Pedido mínimo (produto)_1, Pedido mínimo_900, Prazo de Preparo da Mercadoria_1, PRECO ORIGINAL:38.25, Preço de varejo sugerido (R ${'$'})_95.00, PromoçãoPag_, Selos e Certificações_PEA, Sem Parabenos, ST_Não, SuperOferta_, sync, Tempo de validade_720, topbrands_beauty, Unidade Venda (peso/area/unidade)_unidade, Vegano","searchCategory":"Beleza e Bem-Estar","createdAt":"2024-01-10T23:44:49Z","updatedAt":"2024-02-14T21:02:57Z","publishedAt":"2021-07-21T17:27:13Z","variants":[{"id":43720787689625,"productId":7819670847641,"sku":"afase01200|1100","optionsName":"Default Title","barcode":"7898329484051","price":"38.25","compareAtPrice":"42.50","imageId":null,"inventoryItemId":45820105719961,"inventoryQuantity":7,"inventoryPolicy":"CONTINUE","inventoryManagement":"shopify","fulfillmentService":"manual","firstVariantOption":"Default Title","secondVariantOption":null,"thirdVariantOption":null,"listingPosition":1,"weightInGrams":220,"weightUnit":"g","weight":220.0,"taxable":false,"taxCode":"","createdAt":"2024-01-10T23:44:49Z","updatedAt":"2024-01-10T23:44:51Z","isDefaultVariant":true}],"options":[{"id":9907987742873,"productId":7819670847641,"name":"Title","listingPosition":1,"optionValues":["Default Title"]}],"images":[{"id":37591005429913,"productId":7819670847641,"variantIds":[],"src":"https://cdn.shopify.com/s/files/1/0556/2440/1049/files/afase01200.png?v=1704930293","width":1000,"height":1000,"listingPosition":1,"createdAt":"2024-01-10T23:44:51Z","updatedAt":"2024-01-10T23:44:53Z"},{"id":37591005331609,"productId":7819670847641,"variantIds":[],"src":"https://cdn.shopify.com/s/files/1/0556/2440/1049/files/agua_aromatica_sinergia_receber_alchi_four_elements_200ml_97_2_537664cd94c6d9f36c4e1ff2086b33f8.jpg?v=1704930293","width":1200,"height":1200,"listingPosition":2,"createdAt":"2024-01-10T23:44:51Z","updatedAt":"2024-01-10T23:44:53Z"},{"id":37591005364377,"productId":7819670847641,"variantIds":[],"src":"https://cdn.shopify.com/s/files/1/0556/2440/1049/files/2_1bfd57e5-3ab8-46fb-9639-156a444f05e7.jpg?v=1704930293","width":1046,"height":810,"listingPosition":3,"createdAt":"2024-01-10T23:44:51Z","updatedAt":"2024-01-10T23:44:53Z"}]}"""

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
}
