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
import com.amazonaws.services.dynamodbv2.model.*;
import io.venable.amazonaws.dynamo.table.MissingProvisionedThroughputException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public class TableBuilderTest
{
    private final CreateTableVariationFactory createTableVariationFactory;
    private Random random;
    private String tableName;
    private String hashKeyName;
    private String rangeKeyName;
    private CreateTableVariation createTableVariation;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {new AmazonDynamoDbCreateTableVariationFactory()},
                {new DynamoDbCreateTableVariationFactory()}
        });
    }

    public TableBuilderTest(CreateTableVariationFactory createTableVariationFactory) {
        this.createTableVariationFactory = createTableVariationFactory;
    }

    @Before
    public void setUp()
    {
        random = new Random();
        tableName = UUID.randomUUID().toString();

        hashKeyName = UUID.randomUUID().toString();
        rangeKeyName = UUID.randomUUID().toString();

        createTableVariation = createTableVariationFactory.newCreateTableVariation();
    }

    private TableBuilder createObjectUnderTest()
    {
        return new TableBuilder();
    }

    private TableBuilder createHashOnlyObjectUnderTest()
    {
        // @formatter:off
        return createObjectUnderTest()
                .name(tableName)
                .primary()
                    .hash()
                        .name(hashKeyName).type(ScalarAttributeType.S)
                .and();
        // @formatter:on
    }

    @Test
    public void name_should_return_this()
    {
        TableBuilder objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.name(tableName), is(objectUnderTest));
    }

    @Test
    public void primary_should_return_non_null()
    {
        assertThat(createObjectUnderTest().primary(), notNullValue());
    }

    @Test(expected = MissingProvisionedThroughputException.class)
    public void create_without_read_or_write_capacity_throws_MissingProvisionedThroughputException()
    {
        TableBuilder objectUnderTest = createHashOnlyObjectUnderTest();

        createTableVariation.createTable(objectUnderTest);
    }

    @Test(expected = MissingProvisionedThroughputException.class)
    public void create_without_read_capacity_throws_MissingProvisionedThroughputException()
    {
        TableBuilder objectUnderTest = createHashOnlyObjectUnderTest()
                .primary()
                    .writeCapacity(1)
                .and();
        createTableVariation.createTable(objectUnderTest);
    }

    @Test(expected = MissingProvisionedThroughputException.class)
    public void create_without_write_capacity_throws_MissingProvisionedThroughputException()
    {
        TableBuilder objectUnderTest = createHashOnlyObjectUnderTest()
                .primary()
                    .readCapacity(1)
                .and();
        createTableVariation.createTable(objectUnderTest);
    }

    @Test
    public void create_without_global_secondary_indexes_should_not_set_an_empty_list()
    {
        TableBuilder objectUnderTest = createHashOnlyObjectUnderTest()
                .primary()
                    .readCapacity(1)
                    .writeCapacity(1)
                .and();

        createTableVariation.createTable(objectUnderTest);

        ArgumentCaptor<CreateTableRequest> createTableRequestArgumentCaptor =
                ArgumentCaptor.forClass(CreateTableRequest.class);

        createTableVariation.verifyCreateTable(createTableRequestArgumentCaptor);

        CreateTableRequest createTableRequest = createTableRequestArgumentCaptor.getValue();

        assertThat(createTableRequest, notNullValue());
        assertThat(createTableRequest.getGlobalSecondaryIndexes(), nullValue());
    }

    @Test
    public void full_build()
    {
        String globalIndex1Name = UUID.randomUUID().toString();
        String globalIndex1HashKey = UUID.randomUUID().toString();
        String globalIndex1RangeKey = UUID.randomUUID().toString();

        Integer readCapacity = random.nextInt(50) + 50;
        Integer writeCapacity = readCapacity - random.nextInt(10);

        // @formatter:off
        TableBuilder objectUnderTest = createObjectUnderTest()
                .name(tableName)
                .primary()
                    .hash()
                        .name(hashKeyName).type(ScalarAttributeType.S)
                    .range()
                        .name(rangeKeyName).type(ScalarAttributeType.N)
                    .readCapacity(readCapacity)
                    .writeCapacity(writeCapacity)
                .and()
                .global()
                    .name(globalIndex1Name)
                    .hash()
                        .name(globalIndex1HashKey).type(ScalarAttributeType.N)
                    .range()
                        .name(globalIndex1RangeKey).type(ScalarAttributeType.B)
                    .projection().all()
                    .readCapacity(readCapacity)
                    .writeCapacity(writeCapacity)
                .and();
        // @formatter:on

        createTableVariation.createTable(objectUnderTest);

        ArgumentCaptor<CreateTableRequest> createTableRequestArgumentCaptor =
                ArgumentCaptor.forClass(CreateTableRequest.class);

        createTableVariation.verifyCreateTable(createTableRequestArgumentCaptor);

        CreateTableRequest createTableRequest = createTableRequestArgumentCaptor.getValue();

        assertThat(createTableRequest, notNullValue());
        assertThat(createTableRequest.getTableName(), is(tableName));

        List<KeySchemaElement> keySchema = createTableRequest.getKeySchema();
        assertThat(keySchema, notNullValue());
        assertThat(keySchema.size(), is(2));
        assertThat(keySchema.get(0), notNullValue());
        assertThat(keySchema.get(0).getAttributeName(), is(hashKeyName));
        assertThat(keySchema.get(0).getKeyType(), is(KeyType.HASH.toString()));
        assertThat(keySchema.get(1), notNullValue());
        assertThat(keySchema.get(1).getAttributeName(), is(rangeKeyName));
        assertThat(keySchema.get(1).getKeyType(), is(KeyType.RANGE.toString()));

        List<AttributeDefinition> attributeDefinitions = createTableRequest.getAttributeDefinitions();
        assertThat(attributeDefinitions, notNullValue());
        assertThat(attributeDefinitions.size(), is(4));

        ProvisionedThroughput provisionedThroughput = createTableRequest.getProvisionedThroughput();
        assertThat(provisionedThroughput, notNullValue());
        assertThat(provisionedThroughput.getReadCapacityUnits(), is((long) readCapacity));
        assertThat(provisionedThroughput.getWriteCapacityUnits(), is((long)writeCapacity));
    }

    private interface CreateTableVariation
    {
        void createTable(TableBuilder tableBuilder);

        void verifyCreateTable(ArgumentCaptor<CreateTableRequest> createTableRequestArgumentCaptor);
    }

    private interface CreateTableVariationFactory
    {
        CreateTableVariation newCreateTableVariation();
    }

    private static class AmazonDynamoDbCreateTableVariationFactory implements CreateTableVariationFactory
    {
        @Override
        public CreateTableVariation newCreateTableVariation() {
            return new AmazonDynamoDbCreateTableVariation();
        }

        private static class AmazonDynamoDbCreateTableVariation implements CreateTableVariation {
            private final AmazonDynamoDB amazonDynamoDB = mock(AmazonDynamoDB.class);

            @Override
            public void createTable(TableBuilder tableBuilder) {
                tableBuilder.create(amazonDynamoDB);
            }

            @Override
            public void verifyCreateTable(ArgumentCaptor<CreateTableRequest> createTableRequestArgumentCaptor) {
                verify(amazonDynamoDB).createTable(createTableRequestArgumentCaptor.capture());
            }
        }
    }

    private static class DynamoDbCreateTableVariationFactory implements CreateTableVariationFactory
    {
        @Override
        public CreateTableVariation newCreateTableVariation() {
            return new DynamoDbCreateTableVariation();
        }

        private static class DynamoDbCreateTableVariation implements CreateTableVariation {
            private final DynamoDB dynamoDB = mock(DynamoDB.class);

            @Override
            public void createTable(TableBuilder tableBuilder) {
                tableBuilder.create(dynamoDB);
            }

            @Override
            public void verifyCreateTable(ArgumentCaptor<CreateTableRequest> createTableRequestArgumentCaptor) {
                verify(dynamoDB).createTable(createTableRequestArgumentCaptor.capture());
            }
        }
    }
}