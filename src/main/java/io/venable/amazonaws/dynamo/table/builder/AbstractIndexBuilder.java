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
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import io.venable.amazonaws.dynamo.table.HashRequiredException;
import io.venable.amazonaws.dynamo.table.MissingProvisionedThroughputException;

import java.util.Collection;

/**
 * @author David Venable
 */
abstract class AbstractIndexBuilder<T extends KeyBuilder> implements KeyBuilder<T>
{
    private final TableBuilder parent;
    protected KeyElementBuilderImpl<T> hashBuilder;
    protected KeyElementBuilderImpl<T> rangeBuilder;
    private Long writeCapacity;
    private Long readCapacity;

    protected AbstractIndexBuilder(TableBuilder parent)
    {
        this.parent = parent;
    }

    @Override
    public KeyElementBuilder<T> hash()
    {
        if(hashBuilder == null)
            hashBuilder = new KeyElementBuilderImpl<T>((T)this, KeyType.HASH);
        return hashBuilder;
    }

    @Override
    public KeyElementBuilder<T> range()
    {
        if(rangeBuilder == null)
            rangeBuilder = new KeyElementBuilderImpl<T>((T)this, KeyType.RANGE);
        return rangeBuilder;

    }

    @Override
    public KeyBuilder<T> writeCapacity(Long writeCapacity)
    {
        this.writeCapacity = writeCapacity;
        return this;
    }

    @Override
    public KeyBuilder<T> writeCapacity(Integer writeCapacity)
    {
        this.writeCapacity = Long.valueOf(writeCapacity);
        return this;
    }

    @Override
    public KeyBuilder<T> readCapacity(Long readCapacity)
    {
        this.readCapacity = readCapacity;
        return this;
    }

    @Override
    public KeyBuilder<T> readCapacity(Integer readCapacity)
    {
        this.readCapacity = Long.valueOf(readCapacity);
        return this;
    }

    @Override
    public TableBuilder and()
    {
        return parent;
    }

    protected void buildKeys(Collection<KeySchemaElement> keySchemaElementCollection, Collection<AttributeDefinition> attributeDefinitionCollection)
    {
        if(hashBuilder == null)
            throw new HashRequiredException();

        hashBuilder.build(keySchemaElementCollection, attributeDefinitionCollection);

        if(rangeBuilder != null)
        {
            rangeBuilder.build(keySchemaElementCollection, attributeDefinitionCollection);
        }
    }

    void setProvisionedThroughput(ProvisionedThroughputSetter provisionedThroughputSetter)
    {
        validateReadAndWriteCapacities();

        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
                .withReadCapacityUnits(readCapacity)
                .withWriteCapacityUnits(writeCapacity);
        provisionedThroughputSetter.setProvisionedThroughput(provisionedThroughput);
    }


    private void validateReadAndWriteCapacities()
    {
        if(this.readCapacity == null || this.writeCapacity == null)
        {
            StringBuilder errorStringBuilder = new StringBuilder();
            if(this.readCapacity == null)
                errorStringBuilder.append("Missing read capacity. ");
            if(this.writeCapacity == null)
                errorStringBuilder.append("Missing write capacity.");

            throw new MissingProvisionedThroughputException(errorStringBuilder.toString());
        }
    }
}
