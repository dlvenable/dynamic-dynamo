/*
 * Copyright (c) 2015 David Venable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.venable.amazonaws.dynamo.table.builder;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;

import java.util.Collection;

/**
 * @author David Venable
 */
class PrimaryKeyBuilderImpl extends AbstractIndexBuilder<PrimaryKeyBuilder> implements PrimaryKeyBuilder
{
    public PrimaryKeyBuilderImpl(TableBuilder tableBuilder)
    {
        super(tableBuilder);
    }

    void buildPrimaryKey(Collection<KeySchemaElement> keySchemaElementCollection, Collection<AttributeDefinition> attributeDefinitionCollection)
    {
        buildKeys(keySchemaElementCollection, attributeDefinitionCollection);
    }
}
