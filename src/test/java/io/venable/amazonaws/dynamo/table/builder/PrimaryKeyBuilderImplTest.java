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
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import io.venable.amazonaws.dynamo.table.HashRequiredException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class PrimaryKeyBuilderImplTest
{
    private TableBuilder tableBuilder;
    private String hashName;
    private String rangeName;

    @Before
    public void setUp()
    {
        tableBuilder = mock(TableBuilder.class);
        hashName = UUID.randomUUID().toString();
        rangeName = UUID.randomUUID().toString();
    }

    private PrimaryKeyBuilderImpl createObjectUnderTest()
    {
        return new PrimaryKeyBuilderImpl(tableBuilder);
    }

    @Test
    public void hash_should_return_non_null_value()
    {
        assertThat(createObjectUnderTest().hash(), notNullValue());
    }

    @Test
    public void hash_should_return_same_instance()
    {
        PrimaryKeyBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.hash(), sameInstance(objectUnderTest.hash()));
    }

    @Test
    public void hash_type_should_return_objectUnderTest()
    {
        PrimaryKeyBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.hash().type(ScalarAttributeType.S), is((PrimaryKeyBuilder) objectUnderTest));
    }

    @Test
    public void range_should_return_non_null_value()
    {
        assertThat(createObjectUnderTest().range(), notNullValue());
    }

    @Test
    public void range_should_return_same_instance()
    {
        PrimaryKeyBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.range(), sameInstance(objectUnderTest.range()));
    }

    @Test
    public void range_type_should_return_objectUnderTest()
    {
        PrimaryKeyBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.range().type(ScalarAttributeType.S), is((PrimaryKeyBuilder) objectUnderTest));
    }

    @Test
    public void and_should_return_TableBuilder()
    {
        assertThat(createObjectUnderTest().and(), is(tableBuilder));
    }

    @Test(expected = HashRequiredException.class)
    public void buildPrimaryKey_should_throw_HashRequiredException_if_hash_is_not_present()
    {
        List<KeySchemaElement> keySchemaElementCollection = new ArrayList<>();
        Collection<AttributeDefinition> attributeDefinitionCollection = new ArrayList<>();

        PrimaryKeyBuilderImpl objectUnderTest = createObjectUnderTest();

        objectUnderTest.range().name(rangeName).type(ScalarAttributeType.S);
        objectUnderTest.buildPrimaryKey(keySchemaElementCollection, attributeDefinitionCollection);
    }

    @Test
    public void buildPrimaryKey_should_add_two_KeySchemaElement_objects_when_range_is_present()
    {
        List<KeySchemaElement> keySchemaElementCollection = new ArrayList<>();
        Collection<AttributeDefinition> attributeDefinitionCollection = new ArrayList<>();

        PrimaryKeyBuilderImpl objectUnderTest = createObjectUnderTest();

        objectUnderTest.hash().name(hashName).type(ScalarAttributeType.S);
        objectUnderTest.range().name(rangeName).type(ScalarAttributeType.S);

        objectUnderTest.buildPrimaryKey(keySchemaElementCollection, attributeDefinitionCollection);

        assertThat(keySchemaElementCollection.size(), is(2));
    }

    @Test
    public void buildPrimaryKey_should_add_one_KeySchemaElement_objects_when_range_is_not_present()
    {
        List<KeySchemaElement> keySchemaElementCollection = new ArrayList<>();
        Collection<AttributeDefinition> attributeDefinitionCollection = new ArrayList<>();

        PrimaryKeyBuilderImpl objectUnderTest = createObjectUnderTest();

        objectUnderTest.hash().name(hashName).type(ScalarAttributeType.S);

        objectUnderTest.buildPrimaryKey(keySchemaElementCollection, attributeDefinitionCollection);

        assertThat(keySchemaElementCollection.size(), is(1));
    }
}