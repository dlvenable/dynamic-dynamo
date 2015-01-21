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

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class GlobalSecondaryIndexBuilderImplTest
{
    private TableBuilder tableBuilder;

    @Before
    public void setUp()
    {
        tableBuilder = mock(TableBuilder.class);
    }

    private GlobalSecondaryIndexBuilderImpl createObjectUnderTest()
    {
        return new GlobalSecondaryIndexBuilderImpl(tableBuilder);
    }

    @Test
    public void name_should_return_this()
    {
        GlobalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.name(UUID.randomUUID().toString()), is((GlobalSecondaryIndexBuilder) objectUnderTest));
    }

    @Test
    public void hash_should_return_non_null_value()
    {
        assertThat(createObjectUnderTest().hash(), notNullValue());
    }

    @Test
    public void hash_should_return_same_instance()
    {
        GlobalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.hash(), sameInstance(objectUnderTest.hash()));
    }

    @Test
    public void hash_type_should_return_objectUnderTest()
    {
        GlobalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.hash().type(ScalarAttributeType.S), is((GlobalSecondaryIndexBuilder) objectUnderTest));
    }

    @Test
    public void range_should_return_non_null_value()
    {
        assertThat(createObjectUnderTest().range(), notNullValue());
    }

    @Test
    public void range_should_return_same_instance()
    {
        GlobalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.range(), sameInstance(objectUnderTest.range()));
    }

    @Test
    public void range_type_should_return_objectUnderTest()
    {
        GlobalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.range().type(ScalarAttributeType.S), is((GlobalSecondaryIndexBuilder) objectUnderTest));
    }

    @Test
    public void projection_should_return_non_null_value()
    {
        assertThat(createObjectUnderTest().projection(), notNullValue());
    }

    @Test
    public void projection_should_return_same_instance()
    {
        GlobalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.projection(), sameInstance(objectUnderTest.projection()));
    }

    @Test
    public void projection_all_should_return_objectUnderTest()
    {
        GlobalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.projection().all(), is((GlobalSecondaryIndexBuilder) objectUnderTest));
    }

    @Test
    public void and_should_return_TableBuilder()
    {
        assertThat(createObjectUnderTest().and(), is(tableBuilder));
    }
}