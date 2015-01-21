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
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import io.venable.amazonaws.dynamo.table.NoProjectionException;

import java.util.Arrays;
import java.util.Collection;

/**
 * @author David Venable
 */
class ProjectionBuilderImpl<T> implements ProjectionBuilder<T>
{
    private final T parent;
    private Projection projection;

    ProjectionBuilderImpl(T parent)
    {
        this.parent = parent;
        projection = new Projection();
    }

    @Override
    public T all()
    {
        projection.setProjectionType(ProjectionType.ALL);
        return parent;
    }

    @Override
    public T keys()
    {
        projection.setProjectionType(ProjectionType.KEYS_ONLY);
        return parent;
    }

    @Override
    public T attributes(String... names)
    {
        return attributes(Arrays.asList(names));
    }

    @Override
    public T attributes(Collection<String> names)
    {
        projection.setProjectionType(ProjectionType.INCLUDE);
        projection.setNonKeyAttributes(names);
        return parent;
    }

    public void build(GlobalSecondaryIndex globalSecondaryIndex)
    {
        if(projection.getProjectionType() == null)
            throw new NoProjectionException();

        globalSecondaryIndex.setProjection(projection);
    }
}
