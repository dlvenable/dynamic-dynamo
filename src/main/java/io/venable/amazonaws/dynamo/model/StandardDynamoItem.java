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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * An internal class providing the standard implementation
 * of {@link DynamoItem}.
 *
 * @author David Venable
 * @since 0.1
 */
class StandardDynamoItem implements DynamoItem
{
    private final Map<String, AttributeValue> map;

    public StandardDynamoItem()
    {
        map = new HashMap<>();
    }

    public StandardDynamoItem(Map<String, AttributeValue> map)
    {
        this.map = map;
    }

    @Override
    public String getString(String attributeName)
    {
        AttributeValue attributeValue = map.get(attributeName);
        if(attributeValue == null)
            return null;

        return attributeValue.getS();
    }

    @Override
    public void putString(String attributeName, String value)
    {
        map.put(attributeName, new AttributeValue().withS(value));
    }

    @Override
    public Long getLong(String attributeName)
    {
        AttributeValue attributeValue = map.get(attributeName);
        if(attributeValue == null)
            return null;

        return Long.parseLong(attributeValue.getN());
    }

    @Override
    public void putLong(String attributeName, Long value)
    {
        putNumber(attributeName, value);
    }

    @Override
    public Integer getInteger(String attributeName)
    {
        AttributeValue attributeValue = map.get(attributeName);
        if(attributeValue == null)
            return null;

        return Integer.parseInt(attributeValue.getN());
    }

    @Override
    public void putInteger(String attributeName, Integer value)
    {
        putNumber(attributeName, value);
    }

    private void putNumber(String attributeName, Number value)
    {
        map.put(attributeName, new AttributeValue().withN(value.toString()));
    }

    @Override
    public int size()
    {
        return map.size();
    }

    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value)
    {
        return map.containsValue(value);
    }

    @Override
    public AttributeValue get(Object key)
    {
        return map.get(key);
    }

    @Override
    public AttributeValue put(String key, AttributeValue value)
    {
        return map.put(key, value);
    }

    @Override
    public AttributeValue remove(Object key)
    {
        return map.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends AttributeValue> m)
    {
        map.putAll(m);
    }

    @Override
    public void clear()
    {
        map.clear();
    }

    @Override
    public Set<String> keySet()
    {
        return map.keySet();
    }

    @Override
    public Collection<AttributeValue> values()
    {
        return map.values();
    }

    @Override
    public Set<Entry<String, AttributeValue>> entrySet()
    {
        return map.entrySet();
    }
}
