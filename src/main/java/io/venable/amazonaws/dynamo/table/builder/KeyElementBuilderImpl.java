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

import java.util.Collection;

/**
 * @author David Venable
 */
class KeyElementBuilderImpl<T> implements KeyElementBuilder<T>
{
    private final T parent;
    private final KeyType keyType;
    private String name;
    private ScalarAttributeType type;

    KeyElementBuilderImpl(T parent, KeyType keyType)
    {
        if(parent == null) throw new IllegalArgumentException("parent");
        if(keyType == null) throw new IllegalArgumentException("keyType");

        this.parent = parent;
        this.keyType = keyType;
    }

    @Override
    public KeyElementBuilder<T> name(String name)
    {
        this.name = name;
        return this;
    }

    @Override
    public T type(ScalarAttributeType type)
    {
        this.type = type;
        return parent;
    }

    void build(Collection<KeySchemaElement> keySchemaElementCollection, Collection<AttributeDefinition> attributeDefinitionCollection)
    {
        if(name == null)
            throw new IncompleteKeyException("name");
        if(type == null)
            throw new IncompleteKeyException("type");

        KeySchemaElement keySchemaElement = new KeySchemaElement()
                .withAttributeName(name)
                .withKeyType(keyType);

        keySchemaElementCollection.add(keySchemaElement);

        AttributeDefinition attributeDefinition = new AttributeDefinition()
                .withAttributeName(name)
                .withAttributeType(type);

        attributeDefinitionCollection.add(attributeDefinition);
    }
}
