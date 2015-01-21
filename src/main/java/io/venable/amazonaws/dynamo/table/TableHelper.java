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
import com.amazonaws.services.dynamodbv2.util.Tables;
import io.venable.amazonaws.dynamo.table.builder.TableBuilder;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author David Venable
 */
public class TableHelper
{
    private static final Set<String> tablesRequested = Collections.synchronizedSet(new HashSet<String>());

    public static void createTableIfNecessary(AmazonDynamoDB amazonDynamoDB, String tableName, TableDefiner tableDefiner)
    {
        if(tablesRequested.contains(tableName))
            return;

        if(Tables.doesTableExist(amazonDynamoDB, tableName))
        {
            tablesRequested.add(tableName);
            return;
        }

        TableBuilder tableBuilder = new TableBuilder().name(tableName);
        tableDefiner.defineTable(tableBuilder);

        tableBuilder.create(amazonDynamoDB);
    }
}
