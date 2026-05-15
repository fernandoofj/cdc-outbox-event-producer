/*******************************************************************************
 * Copyright 2018 Disney Streaming Services
 *
 * Licensed under the Apache License, Version 2.0 (the "Apache License")
 * with the following modification; you may not use this file except in
 * compliance with the Apache License and the following modification to it:
 * Section 6. Trademarks. is deleted and replaced with:
 *
 * 6. Trademarks. This License does not grant permission to use the trade
 * names, trademarks, service marks, or product names of the Licensor
 * and its affiliates, except as required to comply with Section 4(c) of
 * the License and to reproduce the content of the NOTICE file.
 *
 * You may obtain a copy of the Apache License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License with the above modification is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the Apache License for the specific
 * language governing permissions and limitations under the Apache License.
 *
 */
package br.com.fltech.cdc.outbox.publisher.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * wal2json `INSERT` row record. `columns` is the post-image of the
 * inserted row; `schema`/`table` identify the source table. The
 * fields are nullable + default to keep backward compatibility with
 * the legacy V1 list-of-changes deserialisation, which doesn't
 * surface them.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
open class InsertChange @JsonCreator constructor(
    @JsonProperty(value = "kind", required = true)
    private val kindInput: String,
    @param:JsonProperty(value = "schema", required = false)
    val schema: String? = null,
    @param:JsonProperty(value = "table", required = false)
    val table: String? = null,
    @param:JsonProperty(value = "lsn", required = false)
    val lsn: String? = null,
    @param:JsonProperty(value = "columns", required = false)
    val columns: List<Wal2JsonColumn>? = null,
) : Change(kindInput)
