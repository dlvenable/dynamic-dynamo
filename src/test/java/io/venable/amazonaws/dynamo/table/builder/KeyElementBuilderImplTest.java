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

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import io.venable.amazonaws.dynamo.table.IncompleteKeyException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class KeyElementBuilderImplTest
{
    private ArbitraryType parent;
    private KeyType keyType;

    @Before
    public void setUp()
    {
        parent = mock(ArbitraryType.class);
        keyType = KeyType.HASH;
    }

    private static class ArbitraryType
    {}

    private KeyElementBuilderImpl<ArbitraryType> createObjectUnderTest()
    {
        return new KeyElementBuilderImpl<>(parent, keyType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_should_throw_if_parent_is_null()
    {
        parent = null;
        createObjectUnderTest();
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_should_throw_if_keyType_is_null()
    {
        keyType = null;
        createObjectUnderTest();
    }

    @Test
    public void name_should_return_this()
    {
        KeyElementBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.name(UUID.randomUUID().toString()), is((KeyElementBuilder)objectUnderTest));
    }

    @Test
    public void type_should_return_parent()
    {
        KeyElementBuilderImpl<ArbitraryType> objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.type(ScalarAttributeType.S), is(parent));
    }

    @Test
    public void build_should_add_KeySchemaElement_with_name_and_Hash_KeyType_when_Hash()
    {
        String name = UUID.randomUUID().toString();
        KeyElementBuilderImpl<ArbitraryType> objectUnderTest = createObjectUnderTest();
        objectUnderTest.name(name).type(ScalarAttributeType.S);

        List<KeySchemaElement> keySchemaElementCollection = new ArrayList<>();
        Collection<AttributeDefinition> attributeDefinitionCollection = new ArrayList<>();

        objectUnderTest.build(keySchemaElementCollection, attributeDefinitionCollection);

        assertThat(keySchemaElementCollection.size(), is(1));
        KeySchemaElement keySchemaElement = keySchemaElementCollection.get(0);

        assertThat(keySchemaElement.getAttributeName(), is(name));
        assertThat(keySchemaElement.getKeyType(), is(keyType.toString()));
    }

    @Test
    public void build_should_add_KeySchemaElement_with_name_and_Range_KeyType_when_Range()
    {
        String name = UUID.randomUUID().toString();

        keyType = KeyType.RANGE;
        KeyElementBuilderImpl<ArbitraryType> objectUnderTest = createObjectUnderTest();
        objectUnderTest.name(name).type(ScalarAttributeType.S);

        List<KeySchemaElement> keySchemaElementCollection = new ArrayList<>();
        Collection<AttributeDefinition> attributeDefinitionCollection = new ArrayList<>();

        objectUnderTest.build(keySchemaElementCollection, attributeDefinitionCollection);

        assertThat(keySchemaElementCollection.size(), is(1));
        KeySchemaElement keySchemaElement = keySchemaElementCollection.get(0);

        assertThat(keySchemaElement.getAttributeName(), is(name));
        assertThat(keySchemaElement.getKeyType(), is(keyType.toString()));
    }

    @Test(expected = IncompleteKeyException.class)
    public void build_should_throw_IncompleteKeyException_if_the_name_is_not_set()
    {
        KeyElementBuilderImpl<ArbitraryType> objectUnderTest = createObjectUnderTest();
        objectUnderTest.type(ScalarAttributeType.S);

        List<KeySchemaElement> keySchemaElementCollection = new ArrayList<>();
        Collection<AttributeDefinition> attributeDefinitionCollection = new ArrayList<>();

        objectUnderTest.build(keySchemaElementCollection, attributeDefinitionCollection);
    }

    @Test(expected = IncompleteKeyException.class)
    public void build_should_throw_IncompleteKeyException_if_the_type_is_not_set()
    {
        String name = UUID.randomUUID().toString();

        KeyElementBuilderImpl<ArbitraryType> objectUnderTest = createObjectUnderTest();
        objectUnderTest.name(name);

        List<KeySchemaElement> keySchemaElementCollection = new ArrayList<>();
        Collection<AttributeDefinition> attributeDefinitionCollection = new ArrayList<>();

        objectUnderTest.build(keySchemaElementCollection, attributeDefinitionCollection);
    }
}