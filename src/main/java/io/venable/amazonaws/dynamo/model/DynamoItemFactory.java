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

package io.venable.amazonaws.dynamo.model;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Map;

/**
 * A factory class to create instances of {@link DynamoItem}.
 *
 * @author David Venable
 * @since 0.1
 */
public class DynamoItemFactory
{
    /**
     * Creates a new, empty {@link DynamoItem}.
     *
     * @return An empty item
     * @since 0.1
     */
    public static DynamoItem createItem()
    {
        return new StandardDynamoItem();
    }

    /**
     * Creates a new instance of {@link DynamoItem}
     * populated from the Java AWS-SDK representation of a DynamoDB item. That is, from
     * a {@link java.util.Map} of {@link java.lang.String} to {@link com.amazonaws.services.dynamodbv2.model.AttributeValue}.
     *
     * @param item the AWS-SDK DynamoDB item
     * @return a new instance of a DynamoDB item
     * @since 0.1
     */
    public static DynamoItem createItem(Map<String, AttributeValue> item)
    {
        return new StandardDynamoItem(item);
    }
}
