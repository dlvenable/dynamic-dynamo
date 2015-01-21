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

import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import io.venable.amazonaws.dynamo.table.NoProjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class ProjectionBuilderImplTest
{
    private ArbitraryClass parent;

    @Before
    public void setUp()
    {
        parent = mock(ArbitraryClass.class);
    }

    private static class ArbitraryClass
    {}

    private ProjectionBuilderImpl<ArbitraryClass> createObjectUnderTest()
    {
        return new ProjectionBuilderImpl<>(parent);
    }

    @Test
    public void all_should_return_parent()
    {
        assertThat(createObjectUnderTest().all(), is(parent));
    }

    @Test
    public void keys_should_return_parent()
    {
        assertThat(createObjectUnderTest().keys(), is(parent));
    }

    @Test
    public void attributes_should_return_parent()
    {
        assertThat(createObjectUnderTest().attributes(UUID.randomUUID().toString()), is(parent));
    }

    @Test
    public void attributes_with_collection_should_return_parent()
    {
        Collection<String> attributes = new ArrayList<>();
        assertThat(createObjectUnderTest().attributes(attributes), is(parent));
    }

    @Test(expected = NoProjectionException.class)
    public void build_without_any_set_should_throw_NoProjectionException()
    {
        GlobalSecondaryIndex globalSecondaryIndex = mock(GlobalSecondaryIndex.class);
        createObjectUnderTest().build(globalSecondaryIndex);
    }
}