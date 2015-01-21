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
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author David Venable
 */
class GlobalSecondaryIndexBuilderImpl extends AbstractIndexBuilder<GlobalSecondaryIndexBuilder> implements GlobalSecondaryIndexBuilder
{
    private final ProjectionBuilderImpl<GlobalSecondaryIndexBuilder> projection;
    private String indexName;

    GlobalSecondaryIndexBuilderImpl(TableBuilder tableBuilder)
    {
        super(tableBuilder);
        this.projection = new ProjectionBuilderImpl<GlobalSecondaryIndexBuilder>(this);
    }

    @Override
    public GlobalSecondaryIndexBuilder name(String indexName)
    {
        this.indexName = indexName;
        return this;
    }

    @Override
    public ProjectionBuilder<GlobalSecondaryIndexBuilder> projection()
    {
        return projection;
    }

    public void buildSecondaryIndexes(Collection<GlobalSecondaryIndex> globalSecondaryIndexCollection, Collection<AttributeDefinition> attributeDefinitionCollection)
    {
        GlobalSecondaryIndex globalSecondaryIndex = new GlobalSecondaryIndex();

        globalSecondaryIndex.setIndexName(indexName);
        Collection<KeySchemaElement> keySchemaElementCollection = new ArrayList<>();
        buildKeys(keySchemaElementCollection, attributeDefinitionCollection);
        globalSecondaryIndex.setKeySchema(keySchemaElementCollection);

        projection.build(globalSecondaryIndex);

        setProvisionedThroughput(new GlobalSecondaryIndexProvisionedThroughputSetter(globalSecondaryIndex));

        globalSecondaryIndexCollection.add(globalSecondaryIndex);
    }
}
