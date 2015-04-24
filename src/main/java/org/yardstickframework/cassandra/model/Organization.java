/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yardstickframework.cassandra.model;

/**
 * Organization record used for query test.
 */
public class Organization {
    /** Organization ID. */
    private int id;

    /** Organization name. */
    private String name;

    /**
     * Constructs empty organization.
     */
    public Organization() {
        // No-op.
    }

    /**
     * Constructs organization with given ID.
     *
     * @param id Organization ID.
     * @param name Organization name.
     */
    public Organization(int id, String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * @return Organization id.
     */
    public int getId() {
        return id;
    }

    /**
     * @param id Organization id.
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return Organization name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Organization name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return this == o || (o instanceof Organization) && id == ((Organization)o).id;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Organization [id=" + id + ", name=" + name + ']';
    }
}
