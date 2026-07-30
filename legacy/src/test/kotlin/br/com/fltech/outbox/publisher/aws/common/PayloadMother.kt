package br.com.fltech.outbox.publisher.aws.common

object PayloadMother {

    fun build() = Payload(
        id = "id",
        name = "Product name",
        description = "Product description",
        price = 100.0,
        category = "Category"
    )
}

data class Payload(
    val id: String,
    val name: String,
    val description: String,
    val price: Double,
    val category: String
)
