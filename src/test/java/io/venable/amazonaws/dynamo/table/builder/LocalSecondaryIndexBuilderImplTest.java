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

import com.amazonaws.services.dynamodbv2.model.*;
import io.venable.amazonaws.dynamo.table.RangeRequiredException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class LocalSecondaryIndexBuilderImplTest
{
    private TableBuilder tableBuilder;
    private String indexName;
    private String rangeName;

    @Before
    public void setUp() {
        tableBuilder = mock(TableBuilder.class);
        indexName = UUID.randomUUID().toString();
        rangeName = UUID.randomUUID().toString();
    }

    private LocalSecondaryIndexBuilderImpl createObjectUnderTest() {
        return new LocalSecondaryIndexBuilderImpl(tableBuilder);
    }

    @Test
    public void name_should_return_the_objectUnderTest() {
        LocalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.name(indexName), is((LocalSecondaryIndexBuilder) objectUnderTest));
    }

    @Test
    public void range_should_return_non_null_value() {
        assertThat(createObjectUnderTest().range(), notNullValue());
    }

    @Test
    public void range_should_return_same_instance() {
        LocalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.range(), sameInstance(objectUnderTest.range()));
    }

    @Test
    public void range_type_should_return_objectUnderTest() {
        LocalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.range().type(ScalarAttributeType.S), is((LocalSecondaryIndexBuilder) objectUnderTest));
    }

    @Test
    public void projection_should_return_non_null_value() {
        assertThat(createObjectUnderTest().projection(), notNullValue());
    }

    @Test
    public void projection_should_return_same_instance() {
        LocalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.projection(), sameInstance(objectUnderTest.projection()));
    }

    @Test
    public void projection_all_should_return_objectUnderTest() {
        LocalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.projection().all(), is((LocalSecondaryIndexBuilder) objectUnderTest));
    }

    @Test
    public void and_should_return_TableBuilder() {
        assertThat(createObjectUnderTest().and(), is(tableBuilder));
    }

    private LocalSecondaryIndexBuilderImpl createObjectUnderTestBuilt() {
        LocalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();

        // @formatter:off
         objectUnderTest
                .name(indexName)
                .range()
                    .name(rangeName).type(ScalarAttributeType.S)
                .projection()
                    .all();
        // @formatter:on

        return objectUnderTest;
    }

    @Test(expected = RangeRequiredException.class)
    public void buildSecondaryIndexes_should_throw_if_range_not_set() {
        LocalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTest();

        // @formatter:off
         objectUnderTest
                .name(indexName)
                .projection()
                    .all();
        // @formatter:on

        KeySchemaElement primaryHashKeySchemaElement = mock(KeySchemaElement.class);
        List<LocalSecondaryIndex> indexList = new ArrayList<>();
        List<AttributeDefinition> attributeDefinitionCollection = new ArrayList<>();

        objectUnderTest.buildSecondaryIndexes(primaryHashKeySchemaElement, indexList, attributeDefinitionCollection);
    }

    @Test
    public void buildSecondaryIndexes_should_add_a_the_primary_hash_key_to_the_list_of_key_schema_elements() {
        LocalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTestBuilt();

        KeySchemaElement primaryHashKeySchemaElement = mock(KeySchemaElement.class);
        List<LocalSecondaryIndex> indexList = new ArrayList<>();
        List<AttributeDefinition> attributeDefinitionCollection = new ArrayList<>();

        objectUnderTest.buildSecondaryIndexes(primaryHashKeySchemaElement, indexList, attributeDefinitionCollection);

        assertThat(indexList.size(), is(1));
        LocalSecondaryIndex createdIndex = indexList.get(0);

        assertThat(createdIndex, notNullValue());
        assertThat(createdIndex.getKeySchema(), notNullValue());
        assertThat(createdIndex.getKeySchema().size(), is(2));
        assertThat(createdIndex.getKeySchema().get(0), is(primaryHashKeySchemaElement));
    }

    @Test
    public void buildSecondaryIndexes_should_add_a_new_LocalSecondaryIndex_to_the_index_collection() {
        LocalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTestBuilt();

        KeySchemaElement primaryHashKeySchemaElement = mock(KeySchemaElement.class);
        List<LocalSecondaryIndex> indexList = new ArrayList<>();
        List<AttributeDefinition> attributeDefinitionCollection = new ArrayList<>();

        objectUnderTest.buildSecondaryIndexes(primaryHashKeySchemaElement, indexList, attributeDefinitionCollection);

        assertThat(indexList.size(), is(1));
        LocalSecondaryIndex createdIndex = indexList.get(0);

        assertThat(createdIndex, notNullValue());
        assertThat(createdIndex.getKeySchema(), notNullValue());
        assertThat(createdIndex.getKeySchema().size(), is(2));
        KeySchemaElement keySchemaElement = createdIndex.getKeySchema().get(1);
        assertThat(keySchemaElement, notNullValue());
        assertThat(keySchemaElement.getAttributeName(), is(rangeName));
        assertThat(keySchemaElement.getKeyType(), is(KeyType.RANGE.toString()));
        assertThat(createdIndex.getIndexName(), is(indexName));
        assertThat(createdIndex.getProjection(), notNullValue());
    }

    @Test
    public void buildSecondaryIndexes_should_add_a_new_AttributeDefinition_to_the_attribute_collection() {
        LocalSecondaryIndexBuilderImpl objectUnderTest = createObjectUnderTestBuilt();

        List<LocalSecondaryIndex> indexList = new ArrayList<>();
        List<AttributeDefinition> attributeDefinitionCollection = new ArrayList<>();

        KeySchemaElement primaryHashKeySchemaElement = mock(KeySchemaElement.class);
        objectUnderTest.buildSecondaryIndexes(primaryHashKeySchemaElement, indexList, attributeDefinitionCollection);

        assertThat(attributeDefinitionCollection.size(), is(1));
        AttributeDefinition attributeDefinition = attributeDefinitionCollection.get(0);

        assertThat(attributeDefinition, notNullValue());
        assertThat(attributeDefinition.getAttributeName(), is(rangeName));
        assertThat(attributeDefinition.getAttributeType(), is(ScalarAttributeType.S.toString()));
    }
}