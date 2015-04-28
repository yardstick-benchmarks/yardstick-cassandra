/*
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.yardstickframework.cassandra.query;

import com.datastax.driver.core.*;
import org.yardstickframework.*;
import org.yardstickframework.cassandra.model.*;
import org.yardstickframework.cassandra.util.*;

import java.util.*;
import java.util.concurrent.*;

import static org.yardstickframework.BenchmarkUtils.*;

/**
 * Benchmark that performs query operations.
 */
public class CassandraSqlQueryBenchmark extends CassandraQueryAbstractBenchmark {
    /** Number of threads that populate the cache for query test. */
    private static final int POPULATE_QUERY_THREAD_NUM = Runtime.getRuntime().availableProcessors() * 2;

    /** Prepared statement. */
    private PreparedStatement queryPs;

    /** Batch size. */
    public static final int BATCH_SIZE = 1000;

    /** {@inheritDoc} */
    @Override public void setUp(final BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        queryPs = session.prepare("SELECT * FROM Person WHERE salary >= ? AND salary <= ? ALLOW FILTERING")
            .setConsistencyLevel(ConsistencyLevel.ONE);

        println(cfg, "Populating query data...");

        long start = System.nanoTime();

        // Populate persons.
        CassandraBenchmarkUtils.runMultiThreaded(new CassandraBenchmarkRunnable() {
            @Override public void run(int threadIdx) throws Exception {
                List<Person> persons = new ArrayList<>(BATCH_SIZE);

                for (int i = threadIdx; i < args.range() && !Thread.currentThread().isInterrupted();
                     i += POPULATE_QUERY_THREAD_NUM) {
                    persons.add(new Person(i, "firstName" + i, "lastName" + i, i * 1000));

                    if (persons.size() == BATCH_SIZE) {
                        put(persons);

                        persons.clear();
                    }
                }

                if (!persons.isEmpty())
                    put(persons);
            }
        }, POPULATE_QUERY_THREAD_NUM, "populate-query-person");

        println(cfg, "Finished populating query data in " + ((System.nanoTime() - start) / 1_000_000) + "ms.");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        double salary = ThreadLocalRandom.current().nextDouble() * 10_000 * 1000;

        double maxSalary = salary + 1000;

        try {
            Collection<Person> persons = executeQuery(salary, maxSalary);

            for (Person p : persons) {
                if (p.getSalary() < salary || p.getSalary() > maxSalary)
                    throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
                            ", person=" + p + ']');
            }

            return true;
        }
        catch (Exception e ){
            BenchmarkUtils.error("Failed query: " + e.getMessage(), null);

            return true;
        }
    }

    /**
     * @param minSalary Min salary.
     * @param maxSalary Max salary.
     * @return Query results.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private Collection<Person> executeQuery(double minSalary, double maxSalary) throws Exception {
        List<Row> rows = session.execute(queryPs.bind(minSalary, maxSalary)).all();

        List<Person> persons = new ArrayList<>(rows.size());

        for (Row row : rows)
            persons.add(new Person(row.getInt(0), row.getString(2), row.getString(3), row.getDouble(1)));

        return persons;
    }
}
