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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.Tables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class TableBuilderIT
{
    private AmazonDynamoDBClient amazonDynamoDB;
    private String tableName;
    private String hashKeyName;
    private String rangeKeyName;
    private String hashKeyValue;
    private String rangeKeyValue;
    private Map<String, AttributeValue> item;
    private String nonIndexedName;
    private String nonIndexedValue;

    @Before
    public void setUp()
    {
        amazonDynamoDB = new AmazonDynamoDBClient();
        amazonDynamoDB.setEndpoint("http://localhost:8000");

        tableName = UUID.randomUUID().toString();

        hashKeyName = UUID.randomUUID().toString();
        rangeKeyName = UUID.randomUUID().toString();

        hashKeyValue = UUID.randomUUID().toString();
        rangeKeyValue = UUID.randomUUID().toString();

        if(Tables.doesTableExist(amazonDynamoDB, tableName))
            amazonDynamoDB.deleteTable(tableName);

        item = new HashMap<>();
        item.put(hashKeyName, new AttributeValue().withS(hashKeyValue));
        item.put(rangeKeyName, new AttributeValue().withS(rangeKeyValue));

        nonIndexedName = UUID.randomUUID().toString();
        nonIndexedValue = UUID.randomUUID().toString();
    }

    @After
    public void tearDown()
    {
        if(Tables.doesTableExist(amazonDynamoDB, tableName))
            amazonDynamoDB.deleteTable(tableName);
    }

    private TableBuilder createObjectUnderTest()
    {
        return new TableBuilder();
    }

    private void putItem(Map<String, AttributeValue> item)
    {
        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(tableName)
                .withItem(item);

        amazonDynamoDB.putItem(putItemRequest);
    }

    @Test
    public void create_a_simple_table_with_a_hash_primary_key()
    {
        // @formatter:off
        createObjectUnderTest()
                .name(tableName)
                .primary()
                    .hash()
                        .name(hashKeyName).type(ScalarAttributeType.S)
                    .readCapacity(1)
                    .writeCapacity(1)
                .and()
                .create(amazonDynamoDB);
        // @formatter:on

        item.remove(rangeKeyName);
        item.put(nonIndexedName, new AttributeValue().withS(nonIndexedValue));
        putItem(item);

        Map<String, AttributeValue> hashToRetrieve = new HashMap<>();
        hashToRetrieve.put(hashKeyName, new AttributeValue().withS(hashKeyValue));

        GetItemRequest getItemByPrimaryKey = new GetItemRequest()
                .withTableName(tableName)
                .withKey(hashToRetrieve);

        GetItemResult resultForItemByPrimaryKey = amazonDynamoDB.getItem(getItemByPrimaryKey);
        Map<String, AttributeValue> itemByPrimaryKey = resultForItemByPrimaryKey.getItem();

        assertThat(itemByPrimaryKey, notNullValue());
        assertThat(itemByPrimaryKey.get(hashKeyName).getS(), is(hashKeyValue));
        assertThat(itemByPrimaryKey.get(nonIndexedName).getS(), is(nonIndexedValue));
    }

    @Test
    public void create_a_simple_table_with_a_hash_and_range_primary_key()
    {
        // @formatter:off
        createObjectUnderTest()
                .name(tableName)
                .primary()
                    .hash()
                        .name(hashKeyName).type(ScalarAttributeType.S)
                    .range()
                        .name(rangeKeyName).type(ScalarAttributeType.S)
                    .readCapacity(1)
                    .writeCapacity(1)
                .and()
                .create(amazonDynamoDB);
        // @formatter:on

        putItem(item);

        Map<String, AttributeValue> hashToRetrieve = new HashMap<>();
        hashToRetrieve.put(hashKeyName, new AttributeValue().withS(hashKeyValue));
        hashToRetrieve.put(rangeKeyName, new AttributeValue().withS(rangeKeyValue));

        GetItemRequest getItemByPrimaryKey = new GetItemRequest()
                .withTableName(tableName)
                .withKey(hashToRetrieve);

        GetItemResult resultForItemByPrimaryKey = amazonDynamoDB.getItem(getItemByPrimaryKey);
        Map<String, AttributeValue> itemByPrimaryKey = resultForItemByPrimaryKey.getItem();

        assertThat(itemByPrimaryKey, notNullValue());
        assertThat(itemByPrimaryKey.get(hashKeyName).getS(), is(hashKeyValue));
        assertThat(itemByPrimaryKey.get(rangeKeyName).getS(), is(rangeKeyValue));
    }

    @Test
    public void create_a_table_with_a_global_secondary_index_using_hash_key()
    {
        String globalSecondaryIndexName = UUID.randomUUID().toString();
        String globalSecondaryHashKeyName = UUID.randomUUID().toString();
        // @formatter:off
        createObjectUnderTest()
                .name(tableName)
                .primary()
                    .hash()
                        .name(hashKeyName).type(ScalarAttributeType.S)
                    .range()
                        .name(rangeKeyName).type(ScalarAttributeType.S)
                    .readCapacity(1)
                    .writeCapacity(1)
                .and()
                .global()
                    .name(globalSecondaryIndexName)
                    .hash()
                        .name(globalSecondaryHashKeyName).type(ScalarAttributeType.S)
                    .projection()
                        .all()
                    .readCapacity(1)
                    .writeCapacity(1)
                .and()
                .create(amazonDynamoDB);
        // @formatter:on

        String globalIndexedValue = UUID.randomUUID().toString();
        item.put(globalSecondaryHashKeyName, new AttributeValue().withS(globalIndexedValue));
        putItem(item);

        Condition equalityCondition = new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withS(globalIndexedValue));
        Map<String, Condition> conditionMap = new HashMap<>();
        conditionMap.put(globalSecondaryHashKeyName, equalityCondition);
        QueryRequest queryRequest = new QueryRequest()
                .withTableName(tableName)
                .withIndexName(globalSecondaryIndexName)
                .withKeyConditions(conditionMap);

        QueryResult queryResult = amazonDynamoDB.query(queryRequest);
        assertThat(queryResult.getCount(), is(1));
        List<Map<String, AttributeValue>> items = queryResult.getItems();
        assertThat(items.size(), is(1));
        Map<String, AttributeValue> singleItem = items.get(0);
        assertThat(singleItem, notNullValue());

        AttributeValue globalHashAttributeValue = singleItem.get(globalSecondaryHashKeyName);
        assertThat(globalHashAttributeValue, notNullValue());
        assertThat(globalHashAttributeValue.getS(), is(globalIndexedValue));

        AttributeValue primaryHashAttributeValue = singleItem.get(hashKeyName);
        assertThat(primaryHashAttributeValue, notNullValue());
        assertThat(primaryHashAttributeValue.getS(), is(hashKeyValue));
    }

    @Test
    public void create_a_table_with_a_global_secondary_index_using_hash_and_range_key()
    {
        String globalSecondaryIndexName = UUID.randomUUID().toString();
        String globalSecondaryHashKeyName = UUID.randomUUID().toString();
        String globalSecondaryRangeKeyName = UUID.randomUUID().toString();
        // @formatter:off
        createObjectUnderTest()
                .name(tableName)
                .primary()
                    .hash()
                        .name(hashKeyName).type(ScalarAttributeType.S)
                    .range()
                        .name(rangeKeyName).type(ScalarAttributeType.S)
                    .readCapacity(1)
                    .writeCapacity(1)
                .and()
                .global()
                    .name(globalSecondaryIndexName)
                    .hash()
                        .name(globalSecondaryHashKeyName).type(ScalarAttributeType.S)
                    .range()
                        .name(globalSecondaryRangeKeyName).type(ScalarAttributeType.N)
                    .projection()
                        .all()
                    .readCapacity(1)
                    .writeCapacity(1)
                .and()
                .create(amazonDynamoDB);
        // @formatter:on

        long rangeNumericalValue = 57;
        String globalIndexedValue = UUID.randomUUID().toString();
        String globalIndexedRangeValue = Long.toString(rangeNumericalValue);
        item.put(globalSecondaryHashKeyName, new AttributeValue().withS(globalIndexedValue));
        item.put(globalSecondaryRangeKeyName, new AttributeValue().withN(globalIndexedRangeValue));
        putItem(item);

        Condition equalityCondition = new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withS(globalIndexedValue));
        Condition rangeCondition = new Condition()
                .withComparisonOperator(ComparisonOperator.BETWEEN)
                .withAttributeValueList(new AttributeValue().withN(Long.toString(rangeNumericalValue - 1)),
                        new AttributeValue().withN(Long.toString(rangeNumericalValue + 1)));

        Map<String, Condition> conditionMap = new HashMap<>();
        conditionMap.put(globalSecondaryHashKeyName, equalityCondition);
        conditionMap.put(globalSecondaryRangeKeyName, rangeCondition);
        QueryRequest queryRequest = new QueryRequest()
                .withTableName(tableName)
                .withIndexName(globalSecondaryIndexName)
                .withKeyConditions(conditionMap);

        QueryResult queryResult = amazonDynamoDB.query(queryRequest);
        assertThat(queryResult.getCount(), is(1));
        List<Map<String, AttributeValue>> items = queryResult.getItems();
        assertThat(items.size(), is(1));
        Map<String, AttributeValue> singleItem = items.get(0);
        assertThat(singleItem, notNullValue());

        AttributeValue globalHashAttributeValue = singleItem.get(globalSecondaryHashKeyName);
        assertThat(globalHashAttributeValue, notNullValue());
        assertThat(globalHashAttributeValue.getS(), is(globalIndexedValue));

        AttributeValue globalRangeAttributeValue = singleItem.get(globalSecondaryRangeKeyName);
        assertThat(globalRangeAttributeValue, notNullValue());
        assertThat(globalRangeAttributeValue.getN(), is(globalIndexedRangeValue));

        AttributeValue primaryHashAttributeValue = singleItem.get(hashKeyName);
        assertThat(primaryHashAttributeValue, notNullValue());
        assertThat(primaryHashAttributeValue.getS(), is(hashKeyValue));
    }
}