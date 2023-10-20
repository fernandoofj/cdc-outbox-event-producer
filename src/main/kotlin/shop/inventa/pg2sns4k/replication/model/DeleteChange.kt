package shop.inventa.pg2sns4k.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty

class DeleteChange @JsonCreator constructor(
    @JsonProperty(value = "kind", required = true)
    private val kindInput: String,
    @param:JsonProperty(value = "table", required = true)
    val table: String,
    @param:JsonProperty(value = "schema", required = true)
    val schema: String,
    @param:JsonProperty(value = "oldkeys", required = true)
    val oldkeys: OldKeys
) : Change(kindInput) {

    @JsonIgnore
    val columnNames = oldkeys.keyNames

    @JsonIgnore
    val columnValues = oldkeys.keyValues

    @Throws(UnknownColumnNameException::class)
    fun getValueForColumn(columnName: String): Any {
        val columnIndex = columnNames.indexOf(columnName)
        return if (columnIndex != -1) {
            columnValues[columnIndex]
        } else {
            throw UnknownColumnNameException(columnName)
        }
    }
}
