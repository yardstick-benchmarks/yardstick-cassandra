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

package org.yardstickframework.cassandra.cache;

import com.datastax.driver.core.*;
import org.yardstickframework.*;
import org.yardstickframework.cassandra.*;
import org.yardstickframework.cassandra.model.*;

import java.util.*;

/**
 * Abstract cache benchmark.
 */
public abstract class CassandraCacheAbstractBenchmark extends CassandraAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        session.execute("CREATE TABLE SampleValue (" +
            "  keyValue int PRIMARY KEY" +
            ");");
    }

    /**
     * @param sampleValue Sample value.
     */
    protected void insert(SampleValue sampleValue) {
        session.execute("INSERT INTO SampleValue (keyValue) VALUES (?)", sampleValue.getId());
    }

    /**
     * @param key Key.
     * @return Sample value.
     */
    protected SampleValue select(int key) {
        ResultSet result = session.execute("SELECT * FROM SampleValue WHERE keyValue = ?", key);

        List<Row> rows = result.all();

        if (rows.isEmpty())
            return null;

        if (rows.size() != 1)
            throw new RuntimeException("Invalid values retrieved. Result: [" + rows + "]");

        return new SampleValue(rows.get(0).getInt("keyValue"));
    }
}
