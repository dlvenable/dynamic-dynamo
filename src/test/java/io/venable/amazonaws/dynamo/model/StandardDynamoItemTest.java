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
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

public class StandardDynamoItemTest
{
    private Map<String, AttributeValue> innerItem;
    private Random random;
    private Object object;
    private AttributeValue attributeValue;
    private String key;
    private String attributeName;

    @Before
    public void setUp()
    {
        innerItem = mock(Map.class);
        random = new Random();
        object = mock(Object.class);
        attributeValue = mock(AttributeValue.class);
        key = UUID.randomUUID().toString();
        attributeName = UUID.randomUUID().toString();
    }

    private DynamoItem createObjectUnderTest()
    {
        return new StandardDynamoItem(innerItem);
    }

    @Test
    public void putString_should_put_AttributeValue_as_string_on_inner_item()
    {
        final String value = UUID.randomUUID().toString();

        createObjectUnderTest().putString(attributeName, value);

        verify(innerItem).put(eq(attributeName), argThat(new BaseMatcher<AttributeValue>()
        {
            @Override
            public boolean matches(Object o)
            {
                AttributeValue providedAttributeValue = (AttributeValue) o;
                return providedAttributeValue.getS().equals(value);
            }

            @Override
            public void describeTo(Description description)
            { }
        }));
    }

    @Test
    public void getString_should_return_string_AttributeValue_from_inner_item()
    {
        final String value = UUID.randomUUID().toString();
        stub(attributeValue.getS()).toReturn(value);
        stub(innerItem.get(attributeName)).toReturn(attributeValue);

        assertThat(createObjectUnderTest().getString(attributeName), is(value));
    }

    @Test
    public void putLong_should_put_AttributeValue_as_long_on_inner_item()
    {
        final Long value = random.nextLong();

        createObjectUnderTest().putLong(attributeName, value);

        verify(innerItem).put(eq(attributeName), argThat(new BaseMatcher<AttributeValue>()
        {
            @Override
            public boolean matches(Object o)
            {
                AttributeValue providedAttributeValue = (AttributeValue) o;
                return Long.parseLong(providedAttributeValue.getN()) == value;
            }

            @Override
            public void describeTo(Description description)
            { }
        }));
    }

    @Test
    public void getLong_should_return_long_AttributeValue_from_inner_item()
    {
        final Long value = random.nextLong();
        stub(attributeValue.getN()).toReturn(value.toString());
        stub(innerItem.get(attributeName)).toReturn(attributeValue);

        assertThat(createObjectUnderTest().getLong(attributeName), is(value));
    }

    @Test
    public void putInteger_should_put_AttributeValue_as_integeer_on_inner_item()
    {
        final Integer value = random.nextInt();

        createObjectUnderTest().putInteger(attributeName, value);

        verify(innerItem).put(eq(attributeName), argThat(new BaseMatcher<AttributeValue>()
        {
            @Override
            public boolean matches(Object o)
            {
                AttributeValue providedAttributeValue = (AttributeValue) o;
                return Integer.parseInt(providedAttributeValue.getN()) == value;
            }

            @Override
            public void describeTo(Description description)
            { }
        }));
    }

    @Test
    public void getInteger_should_return_integer_AttributeValue_from_inner_item()
    {
        final Integer value = random.nextInt();
        stub(attributeValue.getN()).toReturn(value.toString());
        stub(innerItem.get(attributeName)).toReturn(attributeValue);

        assertThat(createObjectUnderTest().getInteger(attributeName), is(value));
    }

    @Test
    public void size_should_return_inner_map_size()
    {
        int size = random.nextInt(55) + 3;

        stub(innerItem.size()).toReturn(size);

        assertThat(createObjectUnderTest().size(), is(size));
    }

    @Test
    public void isEmpty_should_return_true_if_inner_map_isEmpty_returns_true()
    {
        stub(innerItem.isEmpty()).toReturn(true);

        assertThat(createObjectUnderTest().isEmpty(), is(true));
    }

    @Test
    public void isEmpty_should_return_false_if_inner_map_isEmpty_returns_false()
    {
        stub(innerItem.isEmpty()).toReturn(false);

        assertThat(createObjectUnderTest().isEmpty(), is(false));
    }

    @Test
    public void containsKey_should_return_true_if_inner_map_containsKey_returns_true()
    {
        stub(innerItem.containsKey(object)).toReturn(true);

        assertThat(createObjectUnderTest().containsKey(object), is(true));
    }

    @Test
    public void containsKey_should_return_false_if_inner_map_containsKey_returns_false()
    {
        stub(innerItem.containsKey(object)).toReturn(false);

        assertThat(createObjectUnderTest().containsKey(object), is(false));
    }

    @Test
    public void containsValue_should_return_true_if_inner_map_containsValue_returns_true()
    {
        stub(innerItem.containsValue(object)).toReturn(true);

        assertThat(createObjectUnderTest().containsValue(object), is(true));
    }

    @Test
    public void containsValue_should_return_false_if_inner_map_containsValue_returns_false()
    {
        stub(innerItem.containsValue(object)).toReturn(false);

        assertThat(createObjectUnderTest().containsValue(object), is(false));
    }

    @Test
    public void get_should_return_inner_map_get()
    {
        stub(innerItem.get(object)).toReturn(attributeValue);

        assertThat(createObjectUnderTest().get(object), is(attributeValue));
    }

    @Test
    public void put_should_return_inner_map_put()
    {
        stub(innerItem.put(key, attributeValue)).toReturn(attributeValue);

        assertThat(createObjectUnderTest().put(key, attributeValue), is(attributeValue));
        verify(innerItem).put(key, attributeValue);
    }

    @Test
    public void remove_should_call_inner_map_remove()
    {
        createObjectUnderTest().remove(object);
        verify(innerItem).remove(object);
    }

    @Test
    public void putAll_should_call_inner_map_putAll()
    {
        Map<String, AttributeValue> otherMap = mock(Map.class);
        createObjectUnderTest().putAll(otherMap);

        verify(innerItem).putAll(otherMap);
    }

    @Test
    public void clear_should_call_inner_map_clear()
    {
        createObjectUnderTest().clear();

        verify(innerItem).clear();
    }

    @Test
    public void keySet_should_return_inner_map_keySet()
    {
        Set<String> keySet = mock(Set.class);
        stub(innerItem.keySet()).toReturn(keySet);

        assertThat(createObjectUnderTest().keySet(), is(keySet));
    }

    @Test
    public void values_should_return_inner_map_values()
    {
        Collection<AttributeValue> values = mock(Collection.class);
        stub(innerItem.values()).toReturn(values);

        assertThat(createObjectUnderTest().values(), is(values));
    }

    @Test
    public void entrySet_should_return_inner_map_entrySet()
    {
        Set<Map.Entry<String, AttributeValue>> entrySet = mock(Set.class);
        stub(innerItem.entrySet()).toReturn(entrySet);

        assertThat(createObjectUnderTest().entrySet(), is(entrySet));
    }
}