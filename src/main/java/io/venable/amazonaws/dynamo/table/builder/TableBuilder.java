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
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A fluent-style builder for DynamoDB tables.
 *
 * @author David Venable
 * @since 0.1
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

    /**
     * Sets the name of the table.
     *
     * @param tableName the table name
     * @return this {@link TableBuilder}
     * @since 0.1
     */
    public TableBuilder name(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    /**
     * Provides a {@link PrimaryKeyBuilder} for building
     * the primary key of this table.
     *
     * @return the {@link PrimaryKeyBuilder}
     * @since 0.1
     */
    public PrimaryKeyBuilder primary()
    {
        return primaryKeyBuilder;
    }

    /**
     * Provides a {@link GlobalSecondaryIndexBuilder} to create
     * a single global secondary index on this table.
     * <p>
     * Each global secondary index should have a new call to
     * this function.
     *
     * @return the new {@link GlobalSecondaryIndexBuilder}
     * @since 0.1
     */
    public GlobalSecondaryIndexBuilder global()
    {
        GlobalSecondaryIndexBuilderImpl globalSecondaryIndexBuilder = new GlobalSecondaryIndexBuilderImpl(this);
        globalSecondaryIndexBuilderCollection.add(globalSecondaryIndexBuilder);
        return globalSecondaryIndexBuilder;
    }

    /**
     * Provides a {@link LocalSecondaryIndexBuilder} to create
     * a single local secondary index on this table.
     * <p>
     * Each local secondary index should have a new call to
     * this function.
     *
     * @return the new {@link LocalSecondaryIndexBuilder}
     * @since 0.2
     */
    public LocalSecondaryIndexBuilder local()
    {
        LocalSecondaryIndexBuilderImpl localSecondaryIndexBuilder = new LocalSecondaryIndexBuilderImpl(this);
        localSecondaryIndexBuilderCollection.add(localSecondaryIndexBuilder);
        return localSecondaryIndexBuilder;
    }

    /**
     * Creates the table for the given {@link AmazonDynamoDB}.
     *
     * @param amazonDynamoDB the {@link AmazonDynamoDB} instance
     * @return the {@link CreateTableResult} from the create request
     * @since 0.1
     */
    public CreateTableResult create(AmazonDynamoDB amazonDynamoDB)
    {
        CreateTableRequest createTableRequest = buildCreateTableRequest();
        return amazonDynamoDB.createTable(createTableRequest);
    }

    /**
     * Creates the table for the given {@link DynamoDB}.
     *
     * @param dynamoDB the {@link DynamoDB} instance
     * @return the {@link CreateTableResult} from the create request
     * @since 0.2
     */
    public Table create(DynamoDB dynamoDB)
    {
        CreateTableRequest createTableRequest = buildCreateTableRequest();
        return dynamoDB.createTable(createTableRequest);
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

    private KeySchemaElement getHashKeySchemaElement(Collection<KeySchemaElement> keySchemaElementCollection)
    {
        for (KeySchemaElement keySchemaElement : keySchemaElementCollection) {
            if(KeyType.HASH.toString().equals(keySchemaElement.getKeyType()))
                return keySchemaElement;
        }

        throw new IllegalStateException("The hash key was not found, but should have already been created.");
    }
}
