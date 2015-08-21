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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author David Venable
 */
public class TableBuilder
{
    private final PrimaryKeyBuilderImpl primaryKeyBuilder;
    private final Collection<GlobalSecondaryIndexBuilderImpl> globalSecondaryIndexBuilderCollection;
    private final Collection<LocalSecondaryIndexBuilderImpl> localSecondaryIndexBuilderCollection;
    private String tableName;

    public TableBuilder()
    {
        primaryKeyBuilder = new PrimaryKeyBuilderImpl(this);
        globalSecondaryIndexBuilderCollection = new ArrayList<>();
        localSecondaryIndexBuilderCollection = new ArrayList<>();
    }

    public TableBuilder name(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    public PrimaryKeyBuilder primary()
    {
        return primaryKeyBuilder;
    }

    public GlobalSecondaryIndexBuilder global()
    {
        GlobalSecondaryIndexBuilderImpl globalSecondaryIndexBuilder = new GlobalSecondaryIndexBuilderImpl(this);
        globalSecondaryIndexBuilderCollection.add(globalSecondaryIndexBuilder);
        return globalSecondaryIndexBuilder;
    }

    public LocalSecondaryIndexBuilder local()
    {
        LocalSecondaryIndexBuilderImpl localSecondaryIndexBuilder = new LocalSecondaryIndexBuilderImpl(this);
        localSecondaryIndexBuilderCollection.add(localSecondaryIndexBuilder);
        return localSecondaryIndexBuilder;
    }

    public CreateTableResult create(AmazonDynamoDB amazonDynamoDB)
    {
        CreateTableRequest createTableRequest = buildCreateTableRequest();
        return amazonDynamoDB.createTable(createTableRequest);
    }

    private CreateTableRequest buildCreateTableRequest()
    {
        Collection<KeySchemaElement> keySchemaElementCollection = new ArrayList<>();
        Collection<AttributeDefinition> attributeDefinitionCollection = new ArrayList<>();

        primaryKeyBuilder.buildPrimaryKey(keySchemaElementCollection, attributeDefinitionCollection);

        KeySchemaElement primaryHashKeySchemaElement = getHashKeySchemaElement(keySchemaElementCollection);

        Collection<GlobalSecondaryIndex> globalSecondaryIndexCollection = new ArrayList<>();
        for (GlobalSecondaryIndexBuilderImpl globalSecondaryIndexBuilder : globalSecondaryIndexBuilderCollection)
        {
            globalSecondaryIndexBuilder.buildSecondaryIndexes(globalSecondaryIndexCollection, attributeDefinitionCollection);
        }

        Collection<LocalSecondaryIndex> localSecondaryIndexCollection = new ArrayList<>();
        for (LocalSecondaryIndexBuilderImpl localSecondaryIndexBuilder : localSecondaryIndexBuilderCollection)
        {
            localSecondaryIndexBuilder.buildSecondaryIndexes(primaryHashKeySchemaElement, localSecondaryIndexCollection, attributeDefinitionCollection);
        }

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(keySchemaElementCollection)
                .withAttributeDefinitions(attributeDefinitionCollection);

        if(globalSecondaryIndexCollection.size() > 0)
            createTableRequest.setGlobalSecondaryIndexes(globalSecondaryIndexCollection);

        if(localSecondaryIndexCollection.size() > 0)
            createTableRequest.setLocalSecondaryIndexes(localSecondaryIndexCollection);

        primaryKeyBuilder.setProvisionedThroughput(new PrimaryKeyProvisionedThroughputSetter(createTableRequest));

        return createTableRequest;
    }

    public KeySchemaElement getHashKeySchemaElement(Collection<KeySchemaElement> keySchemaElementCollection)
    {
        for (KeySchemaElement keySchemaElement : keySchemaElementCollection) {
            if(KeyType.HASH.toString().equals(keySchemaElement.getKeyType()))
                return keySchemaElement;
        }

        throw new IllegalStateException("The hash key was not found, but should have already been created.");
    }
}
