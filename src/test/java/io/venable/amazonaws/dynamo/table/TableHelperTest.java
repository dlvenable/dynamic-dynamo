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

package io.venable.amazonaws.dynamo.table;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import io.venable.amazonaws.dynamo.table.builder.TableBuilder;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Objects;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TableHelperTest
{
    private AmazonDynamoDB amazonDynamoDB;
    private TableDefiner tableDefiner;

    @Before
    public void setUp()
    {
        amazonDynamoDB = mock(AmazonDynamoDB.class);
        tableDefiner = mock(TableDefiner.class);
    }

    private static class DescribeTableMatcher extends BaseMatcher<DescribeTableRequest>
    {
        private final String tableName;

        public DescribeTableMatcher(String tableName)
        {
            Objects.requireNonNull(tableName);
            this.tableName = tableName;
        }

        @Override
        public boolean matches(Object item)
        {
            if(! (item instanceof DescribeTableRequest))
                return false;

            DescribeTableRequest describeTableRequest = (DescribeTableRequest) item;

            return tableName.equals(describeTableRequest.getTableName());
        }

        @Override
        public void describeTo(Description description)
        { }
    }

    private static DescribeTableMatcher requestsToDescribeTable(String tableName)
    {
        return new DescribeTableMatcher(tableName);
    }

    private void stubTableToExistActive(String tableName)
    {
        TableDescription tableDescription = mock(TableDescription.class);
        stub(tableDescription.getTableName()).toReturn(tableName);
        stub(tableDescription.getTableStatus()).toReturn(TableStatus.ACTIVE.toString());

        DescribeTableResult describeTableResult = mock(DescribeTableResult.class);
        stub(describeTableResult.getTable()).toReturn(tableDescription);

        stub(amazonDynamoDB.describeTable(argThat(requestsToDescribeTable(tableName)))).toReturn(describeTableResult);
    }

    private void stubTableToNotExist(String tableName)
    {
        when(amazonDynamoDB.describeTable(argThat(requestsToDescribeTable(tableName)))).thenThrow(ResourceNotFoundException.class);
    }

    @Test
    public void createTableIfNecessary_should_only_check_for_the_table_to_exist_once_per_table_if_it_exists()
    {
        String tableName = UUID.randomUUID().toString();

        stubTableToExistActive(tableName);

        for(int i = 0; i < 5; i++)
            TableHelper.createTableIfNecessary(amazonDynamoDB, tableName, tableDefiner);

        verify(amazonDynamoDB, times(1)).describeTable(any(DescribeTableRequest.class));
    }

    @Test
    public void createTableIfNecessary_should_create_a_table_if_it_does_not_exist()
    {
        String tableName = UUID.randomUUID().toString();

        stubTableToNotExist(tableName);

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                TableBuilder tableBuilder = (TableBuilder) invocationOnMock.getArguments()[0];

                tableBuilder.primary().hash().name(UUID.randomUUID().toString()).type(ScalarAttributeType.S);
                tableBuilder.primary().readCapacity(1).writeCapacity(1);
                return null;
            }
        }).when(tableDefiner).defineTable(any(TableBuilder.class));

        TableHelper.createTableIfNecessary(amazonDynamoDB, tableName, tableDefiner);

        verify(amazonDynamoDB).createTable(any(CreateTableRequest.class));
    }
}