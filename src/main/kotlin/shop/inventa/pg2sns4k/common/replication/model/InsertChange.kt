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
package shop.inventa.pg2sns4k.common.replication.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

open class InsertChange @JsonCreator constructor(
    @JsonProperty(value = "kind", required = true)
    private val kindInput: String,
    @param:JsonProperty(value = "columnnames", required = true)
    val columnNames: List<String>,
    @param:JsonProperty(value = "columntypes", required = true)
    val columnTypes: List<String>,
    @param:JsonProperty(value = "table", required = true)
    val table: String,
    @param:JsonProperty(value = "columnvalues", required = true)
    val columnValues: List<Any>,
    @param:JsonProperty(value = "schema", required = true)
    val schema: String
) : Change(kindInput) {

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
