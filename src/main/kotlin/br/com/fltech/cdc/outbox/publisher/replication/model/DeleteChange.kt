package br.com.fltech.cdc.outbox.publisher.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
class DeleteChange @JsonCreator constructor(
    @JsonProperty(value = "kind", required = true)
    private val kindInput: String
) : Change(kindInput)
