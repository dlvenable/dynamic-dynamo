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

/**
 * A builder which allows for creating a local secondary
 * index on a table.
 *
 * @since 0.2
 * @author David Venable
 */
public interface LocalSecondaryIndexBuilder extends TableBuilderSubComponent
{
    /**
     * Set the name of the index.
     *
     * @param indexName the index name
     * @return this object to allow for continue building
     * @since 0.2
     */
    LocalSecondaryIndexBuilder name(String indexName);

    /**
     * Define the range key to use for this index.
     *
     * @return a {@link KeyBuilder} for defining the range key
     * @since 0.2
     */
    KeyElementBuilder<LocalSecondaryIndexBuilder> range();

    /**
     * Define the project for this index.
     *
     * @return a {@link ProjectionBuilder} for the projection on this index
     * @since 0.2
     */
    ProjectionBuilder<LocalSecondaryIndexBuilder> projection();
}
