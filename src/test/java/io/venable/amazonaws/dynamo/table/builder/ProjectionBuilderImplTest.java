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
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import io.venable.amazonaws.dynamo.table.NoProjectionException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ProjectionBuilderImplTest
{
    private ArbitraryClass parent;
    private LocalSecondaryIndex localSecondaryIndex;
    private GlobalSecondaryIndex globalSecondaryIndex;
    private String attribute;

    @Before
    public void setUp()
    {
        parent = mock(ArbitraryClass.class);
        globalSecondaryIndex = mock(GlobalSecondaryIndex.class);
        localSecondaryIndex = mock(LocalSecondaryIndex.class);
        attribute = UUID.randomUUID().toString();
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
        assertThat(createObjectUnderTest().attributes(attribute), is(parent));
    }

    @Test
    public void attributes_with_collection_should_return_parent()
    {
        Collection<String> attributes = Collections.singletonList(attribute);
        assertThat(createObjectUnderTest().attributes(attributes), is(parent));
    }

    @Test
    public void build_on_global_secondary_index_with_all_projection_should_set_all()
    {
        ProjectionBuilderImpl<ArbitraryClass> objectUnderTest = createObjectUnderTest();
        objectUnderTest.all();
        objectUnderTest.build(globalSecondaryIndex);

        ArgumentCaptor<Projection> projectionArgumentCaptor = ArgumentCaptor.forClass(Projection.class);

        verify(globalSecondaryIndex).setProjection(projectionArgumentCaptor.capture());

        Projection actualProjection = projectionArgumentCaptor.getValue();
        assertThat(actualProjection.getProjectionType(), is(ProjectionType.ALL.toString()));
    }

    @Test
    public void build_on_global_secondary_index_with_keys_projection_should_set_keys()
    {
        ProjectionBuilderImpl<ArbitraryClass> objectUnderTest = createObjectUnderTest();
        objectUnderTest.keys();
        objectUnderTest.build(globalSecondaryIndex);

        ArgumentCaptor<Projection> projectionArgumentCaptor = ArgumentCaptor.forClass(Projection.class);

        verify(globalSecondaryIndex).setProjection(projectionArgumentCaptor.capture());

        Projection actualProjection = projectionArgumentCaptor.getValue();
        assertThat(actualProjection.getProjectionType(), is(ProjectionType.KEYS_ONLY.toString()));
    }

    @Test
    public void build_on_global_secondary_index_with_attributes_projection_should_set_attributes()
    {
        ProjectionBuilderImpl<ArbitraryClass> objectUnderTest = createObjectUnderTest();
        objectUnderTest.attributes(attribute);
        objectUnderTest.build(globalSecondaryIndex);

        ArgumentCaptor<Projection> projectionArgumentCaptor = ArgumentCaptor.forClass(Projection.class);

        verify(globalSecondaryIndex).setProjection(projectionArgumentCaptor.capture());

        Projection actualProjection = projectionArgumentCaptor.getValue();
        assertThat(actualProjection.getProjectionType(), is(ProjectionType.INCLUDE.toString()));
        assertThat(actualProjection.getNonKeyAttributes(), notNullValue());
        assertThat(actualProjection.getNonKeyAttributes().size(), is(1));
        assertThat(actualProjection.getNonKeyAttributes(), hasItem(attribute));
    }

    @Test(expected = NoProjectionException.class)
    public void build_on_global_secondary_index_without_any_set_should_throw_NoProjectionException()
    {
        createObjectUnderTest().build(globalSecondaryIndex);
    }

    @Test
    public void build_on_local_secondary_index_with_all_projection_should_set_all()
    {
        ProjectionBuilderImpl<ArbitraryClass> objectUnderTest = createObjectUnderTest();
        objectUnderTest.all();
        objectUnderTest.build(localSecondaryIndex);

        ArgumentCaptor<Projection> projectionArgumentCaptor = ArgumentCaptor.forClass(Projection.class);

        verify(localSecondaryIndex).setProjection(projectionArgumentCaptor.capture());

        Projection actualProjection = projectionArgumentCaptor.getValue();
        assertThat(actualProjection.getProjectionType(), is(ProjectionType.ALL.toString()));
    }

    @Test
    public void build_on_local_secondary_index_with_keys_projection_should_set_keys()
    {
        ProjectionBuilderImpl<ArbitraryClass> objectUnderTest = createObjectUnderTest();
        objectUnderTest.keys();
        objectUnderTest.build(localSecondaryIndex);

        ArgumentCaptor<Projection> projectionArgumentCaptor = ArgumentCaptor.forClass(Projection.class);

        verify(localSecondaryIndex).setProjection(projectionArgumentCaptor.capture());

        Projection actualProjection = projectionArgumentCaptor.getValue();
        assertThat(actualProjection.getProjectionType(), is(ProjectionType.KEYS_ONLY.toString()));
    }

    @Test
    public void build_on_local_secondary_index_with_attributes_projection_should_set_attributes()
    {
        ProjectionBuilderImpl<ArbitraryClass> objectUnderTest = createObjectUnderTest();
        objectUnderTest.attributes(attribute);
        objectUnderTest.build(localSecondaryIndex);

        ArgumentCaptor<Projection> projectionArgumentCaptor = ArgumentCaptor.forClass(Projection.class);

        verify(localSecondaryIndex).setProjection(projectionArgumentCaptor.capture());

        Projection actualProjection = projectionArgumentCaptor.getValue();
        assertThat(actualProjection.getProjectionType(), is(ProjectionType.INCLUDE.toString()));
        assertThat(actualProjection.getNonKeyAttributes(), notNullValue());
        assertThat(actualProjection.getNonKeyAttributes().size(), is(1));
        assertThat(actualProjection.getNonKeyAttributes(), hasItem(attribute));
    }

    @Test(expected = NoProjectionException.class)
    public void build_on_local_secondary_index_without_any_set_should_throw_NoProjectionException()
    {
        createObjectUnderTest().build(localSecondaryIndex);
    }
}