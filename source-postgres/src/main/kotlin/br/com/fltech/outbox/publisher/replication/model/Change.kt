package br.com.fltech.outbox.publisher.replication.model

import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "kind",
    visible = true,
)
@JsonSubTypes(
    JsonSubTypes.Type(value = InsertChange::class, name = "insert"),
    JsonSubTypes.Type(value = UpdateChange::class, name = "update"),
    JsonSubTypes.Type(value = DeleteChange::class, name = "delete"),
    JsonSubTypes.Type(value = MessageChange::class, name = "message"),
)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
open class Change
    @JsonCreator
    constructor(
        @param:JsonProperty(value = "kind", required = true)
        val kind: String,
    )
