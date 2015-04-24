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

package org.yardstickframework.cassandra.query;

import com.datastax.driver.core.*;
import org.yardstickframework.*;
import org.yardstickframework.cassandra.*;
import org.yardstickframework.cassandra.model.*;

import java.util.*;

/**
 * Abstract query benchmark.
 */
public abstract class CassandraQueryAbstractBenchmark extends CassandraAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        // Init schema.
        session.execute("CREATE TABLE Person (" +
            "  id int," +
            "  firstName varchar," +
            "  lastName varchar," +
            "  salary double," +
            "  PRIMARY KEY (id, salary)" +
            ");"
        );
    }

    /**
     * @param p Person.
     */
    protected void put(Person p) {
        session.execute("INSERT INTO Person (id, firstName, lastName, salary) VALUES (?, ?, ?, ?)",
            p.getId(), p.getFirstName(), p.getLastName(), p.getSalary());
    }

    /**
     * @param persons Persons.
     */
    protected void put(List<Person> persons) {
        PreparedStatement ps = session.prepare("INSERT INTO Person (id, firstName, lastName, salary) " +
            "VALUES (?, ?, ?, ?)");

        BatchStatement batch = new BatchStatement();

        for (Person p : persons)
            batch.add(ps.bind(p.getId(), p.getFirstName(), p.getLastName(), p.getSalary()));

        session.execute(batch);
    }
}
