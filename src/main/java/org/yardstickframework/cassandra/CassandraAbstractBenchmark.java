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

package org.yardstickframework.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import org.yardstickframework.*;

import java.util.concurrent.*;

import static org.yardstickframework.BenchmarkUtils.*;

/**
 * Abstract class for Ignite benchmarks.
 */
public abstract class CassandraAbstractBenchmark extends BenchmarkDriverAdapter {
    /** Arguments. */
    protected final CassandraBenchmarkArguments args = new CassandraBenchmarkArguments();

    /** Working session. */
    protected Session session;

    /** Cluster. */
    private Cluster cluster;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        jcommander(cfg.commandLineArguments(), args, "<cassandra-driver>");

        cluster = Cluster.builder().addContactPoint(cfg.hostName()).build();

        dropKeySpaceQuietly(cluster, args.keySpaceName());

        session = createKeySpace(cluster, args.keySpaceName(), args.backups());
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        session.close();

        cluster.close();
    }

    /** {@inheritDoc} */
    @Override public String description() {
        String desc = BenchmarkUtils.description(cfg, this);

        return desc.isEmpty() ?
            getClass().getSimpleName() + args.description() + cfg.defaultDescription() : desc;
    }

    /** {@inheritDoc} */
    @Override public String usage() {
        return BenchmarkUtils.usage(args);
    }

    /**
     * @param max Key range.
     * @return Next key.
     */
    protected int nextRandom(int max) {
        return ThreadLocalRandom.current().nextInt(max);
    }

    /**
     * @param min Minimum key in range.
     * @param max Maximum key in range.
     * @return Next key.
     */
    protected int nextRandom(int min, int max) {
        return ThreadLocalRandom.current().nextInt(max - min) + min;
    }

    /**
     * Create key space.
     *
     * @param cluster Cluster.
     * @param keySpaceName Key space.
     * @param backups Count backups.
     * @return Session.
     */
    protected Session createKeySpace(Cluster cluster, String keySpaceName, int backups) {
        try (Session session = cluster.connect()) {
            session.execute("CREATE KEYSPACE " + keySpaceName
                + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : " + backups + " };");
        }

        return cluster.connect(keySpaceName);
    }

    /**
     * Drop key space quietly.
     *
     * @param cluster Cluster.
     * @param keySpaceName Key space name.
     */
    protected void dropKeySpaceQuietly(Cluster cluster, String keySpaceName) {
        try (Session session = cluster.connect()) {
            session.execute("DROP KEYSPACE " + keySpaceName + ";");
        }
        catch (InvalidQueryException ignore) {
            // No-op.
        }
    }
}
