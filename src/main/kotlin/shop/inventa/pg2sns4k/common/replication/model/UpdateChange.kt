package shop.inventa.pg2sns4k.common.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

@Suppress("LongParameterList")
class UpdateChange @JsonCreator constructor(
    @JsonProperty(value = "kind", required = true)
    private val kindInput: String,
    @JsonProperty(value = "columnnames", required = true)
    val columnNamesInput: List<String>,
    @JsonProperty(value = "columntypes", required = true)
    val columnTypesInput: List<String>,
    @JsonProperty(value = "table", required = true)
    val tableInput: String,
    @JsonProperty(value = "columnvalues", required = true)
    val columnValuesInput: List<Any>,
    @JsonProperty(value = "schema", required = true)
    val schemaInput: String,
    @param:JsonProperty(value = "oldkeys", required = true)
    val oldkeys: OldKeys
) : InsertChange(
    kindInput,
    columnNamesInput,
    columnTypesInput,
    tableInput,
    columnValuesInput,
    schemaInput
)
