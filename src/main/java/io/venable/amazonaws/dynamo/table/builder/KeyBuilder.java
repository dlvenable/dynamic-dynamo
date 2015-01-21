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
 * @author David Venable
 */
public interface KeyBuilder<T extends KeyBuilder> extends TableBuilderSubComponent
{
    KeyElementBuilder<T> hash();
    KeyElementBuilder<T> range();

    KeyBuilder<T> writeCapacity(Long writeCapacity);
    KeyBuilder<T> writeCapacity(Integer writeCapacity);

    KeyBuilder<T> readCapacity(Long readCapacity);
    KeyBuilder<T> readCapacity(Integer readCapacity);
}
