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
 * This interface represents an item in a DynamoDB database.
 * <p>
 * This interface provides abstractions over the standard Java
 * SDK item. Using it helps avoid creating instances of
 * {@link com.amazonaws.services.dynamodbv2.model.AttributeValue}
 * and performing your own casts.
 *
 * @author David Venable
 * @since 0.1
 */
public interface DynamoItem extends Map<String, AttributeValue>
{
    /**
     * Gets a {@link java.lang.String} value for an attribute.
     *
     * @param attributeName the name of the attribute to get
     * @return the string value for the attribute
     * @since 0.1
     */
    String getString(String attributeName);

    /**
     * Puts a {@link java.lang.String} value into an attribute.
     *
     * @param attributeName the name of the attribute to set
     * @param value the string value to set for the attribute
     * @since 0.1
     */
    void putString(String attributeName, String value);

    /**
     * Gets a {@link java.lang.Long} value for an attribute.
     * <p>
     * DynamoDB only provides one number type which can be used
     * for integers or decimals.
     *
     * @param attributeName the name of the attribute to get
     * @return the long value for the attribute
     * @since 0.1
     */
    Long getLong(String attributeName);

    /**
     * Puts a {@link java.lang.Long} value into an attribute.
     * <p>
     * DynamoDB only provides one number type which can be used
     * for integers or decimals.
     *
     * @param attributeName the name of the attribute to set
     * @param value the long value to set for the attribute
     * @since 0.1
     */
    void putLong(String attributeName, Long value);

    /**
     * Gets an {@link java.lang.Integer} value for an attribute.
     * <p>
     * DynamoDB only provides one number type which can be used
     * for integers or decimals.
     *
     * @param attributeName the name of the attribute to get
     * @return the integer value for the attribute
     * @since 0.1
     */
    Integer getInteger(String attributeName);

    /**
     * Puts an {@link java.lang.Integer} value into an attribute.
     * <p>
     * DynamoDB only provides one number type which can be used
     * for integers or decimals.
     *
     * @param attributeName the name of the attribute to set
     * @param value the long value to set for the attribute
     * @since 0.1
     */
    void putInteger(String attributeName, Integer value);
}
