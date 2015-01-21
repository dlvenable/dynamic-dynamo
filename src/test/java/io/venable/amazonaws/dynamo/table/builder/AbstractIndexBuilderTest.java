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

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import io.venable.amazonaws.dynamo.table.MissingProvisionedThroughputException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AbstractIndexBuilderTest
{
    private ProvisionedThroughputSetter provisionedThroughputSetter;

    private class AbstractIndexBuilderForTesting extends AbstractIndexBuilder<KeyBuilder>
    {
        protected AbstractIndexBuilderForTesting(TableBuilder parent)
        {
            super(parent);
        }
    }

    @Before
    public void setUp()
    {
        provisionedThroughputSetter = mock(ProvisionedThroughputSetter.class);
    }

    private AbstractIndexBuilder<KeyBuilder> createObjectUnderTest()
    {
        TableBuilder tableBuilder = mock(TableBuilder.class);
        return new AbstractIndexBuilderForTesting(tableBuilder);
    }

    @Test
    public void writeCapacity_with_int_should_return_this()
    {
        AbstractIndexBuilder<KeyBuilder> objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.writeCapacity(2), is((KeyBuilder<KeyBuilder>) objectUnderTest));
    }

    @Test
    public void writeCapacity_with_long_should_return_this()
    {
        AbstractIndexBuilder<KeyBuilder> objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.writeCapacity(2l), is((KeyBuilder<KeyBuilder>)objectUnderTest));
    }

    @Test
    public void readCapacity_with_int_should_return_this()
    {
        AbstractIndexBuilder<KeyBuilder> objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.readCapacity(2), is((KeyBuilder<KeyBuilder>)objectUnderTest));
    }

    @Test
    public void readCapacity_with_long_should_return_this()
    {
        AbstractIndexBuilder<KeyBuilder> objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.readCapacity(2l), is((KeyBuilder<KeyBuilder>)objectUnderTest));
    }

    @Test
    public void setProvisionedThroughput_should_call_setProvisionedThroughput_with_read_and_write_capacities()
    {
        Long readCapacity = 11l;
        Long writeCapacity = 7l;

        AbstractIndexBuilder<KeyBuilder> objectUnderTest = createObjectUnderTest();
        objectUnderTest
                .readCapacity(readCapacity)
                .writeCapacity(writeCapacity);

        objectUnderTest.setProvisionedThroughput(provisionedThroughputSetter);

        ArgumentCaptor<ProvisionedThroughput> provisionedThroughputArgumentCaptor =
                ArgumentCaptor.forClass(ProvisionedThroughput.class);
        verify(provisionedThroughputSetter).setProvisionedThroughput(provisionedThroughputArgumentCaptor.capture());

        ProvisionedThroughput provisionedThroughput = provisionedThroughputArgumentCaptor.getValue();
        assertThat(provisionedThroughput.getReadCapacityUnits(), is(readCapacity));
        assertThat(provisionedThroughput.getWriteCapacityUnits(), is(writeCapacity));
    }

    @Test(expected = MissingProvisionedThroughputException.class)
    public void setProvisionedThroughput_without_read_or_write_capacity_throws_MissingProvisionedThroughputException()
    {
        createObjectUnderTest()
                .setProvisionedThroughput(provisionedThroughputSetter);
    }

    @Test(expected = MissingProvisionedThroughputException.class)
    public void setProvisionedThroughput_without_read_capacity_throws_MissingProvisionedThroughputException()
    {
        AbstractIndexBuilder<KeyBuilder> objectUnderTest = createObjectUnderTest();
        objectUnderTest
                .writeCapacity(1);

        objectUnderTest.setProvisionedThroughput(provisionedThroughputSetter);
    }

    @Test(expected = MissingProvisionedThroughputException.class)
    public void setProvisionedThroughput_without_write_capacity_throws_MissingProvisionedThroughputException()
    {
        AbstractIndexBuilder<KeyBuilder> objectUnderTest = createObjectUnderTest();
        objectUnderTest
                .readCapacity(1);

        objectUnderTest.setProvisionedThroughput(provisionedThroughputSetter);
    }

}