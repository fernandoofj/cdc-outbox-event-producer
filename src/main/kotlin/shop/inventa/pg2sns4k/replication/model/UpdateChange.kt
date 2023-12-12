package shop.inventa.pg2sns4k.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
class UpdateChange @JsonCreator constructor(
    @JsonProperty(value = "kind", required = true)
    private val kindInput: String
) : Change(kindInput)
