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
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import io.venable.amazonaws.dynamo.table.RangeRequiredException;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author David Venable
 */
class LocalSecondaryIndexBuilderImpl implements LocalSecondaryIndexBuilder
{
    private final TableBuilder tableBuilder;
    private final ProjectionBuilderImpl<LocalSecondaryIndexBuilder> projection;
    private String indexName;
    private KeyElementBuilderImpl<LocalSecondaryIndexBuilder> rangeBuilder;

    public LocalSecondaryIndexBuilderImpl(TableBuilder tableBuilder) {
        this.tableBuilder = tableBuilder;
        this.projection = new ProjectionBuilderImpl<LocalSecondaryIndexBuilder>(this);
    }

    @Override
    public LocalSecondaryIndexBuilder name(String indexName) {
        this.indexName = indexName;
        return this;
    }

    @Override
    public KeyElementBuilder<LocalSecondaryIndexBuilder> range() {
        if(rangeBuilder == null)
            rangeBuilder = new KeyElementBuilderImpl<LocalSecondaryIndexBuilder>(this, KeyType.RANGE);
        return rangeBuilder;
    }

    @Override
    public ProjectionBuilder<LocalSecondaryIndexBuilder> projection() {
        return projection;
    }

    @Override
    public TableBuilder and() {
        return tableBuilder;
    }

    void buildSecondaryIndexes(KeySchemaElement primaryHashKeySchemaElement, Collection<LocalSecondaryIndex> localSecondaryIndexCollection, Collection<AttributeDefinition> attributeDefinitionCollection)
    {
        LocalSecondaryIndex localSecondaryIndex = new LocalSecondaryIndex();

        localSecondaryIndex.setIndexName(indexName);
        Collection<KeySchemaElement> keySchemaElementCollection = new ArrayList<>();
        keySchemaElementCollection.add(primaryHashKeySchemaElement);
        buildRangeKey(keySchemaElementCollection, attributeDefinitionCollection);
        localSecondaryIndex.setKeySchema(keySchemaElementCollection);

        projection.build(localSecondaryIndex);

        localSecondaryIndexCollection.add(localSecondaryIndex);
    }

    private void buildRangeKey(Collection<KeySchemaElement> keySchemaElementCollection, Collection<AttributeDefinition> attributeDefinitionCollection)
    {
        if(rangeBuilder == null)
            throw new RangeRequiredException();

        rangeBuilder.build(keySchemaElementCollection, attributeDefinitionCollection);
    }
}
