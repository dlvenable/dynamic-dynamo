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
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import org.junit.Before;
import org.junit.Test;

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

    private void stubTableToExistActive()
    {
        TableDescription tableDescription = mock(TableDescription.class);
        stub(tableDescription.getTableStatus()).toReturn(TableStatus.ACTIVE.toString());

        DescribeTableResult describeTableResult = mock(DescribeTableResult.class);
        stub(describeTableResult.getTable()).toReturn(tableDescription);

        stub(amazonDynamoDB.describeTable(any(DescribeTableRequest.class))).toReturn(describeTableResult);
    }

    @Test
    public void createTableIfNecessary_should_only_check_for_the_table_to_exist_once_per_table_if_it_exists()
    {
        String tableName = UUID.randomUUID().toString();

        stubTableToExistActive();

        for(int i = 0; i < 5; i++)
            TableHelper.createTableIfNecessary(amazonDynamoDB, tableName, tableDefiner);

        verify(amazonDynamoDB, times(1)).describeTable(any(DescribeTableRequest.class));
    }
}