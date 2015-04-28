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

import java.util.*;
import java.util.concurrent.*;

/**
 * Benchmark that performs put and query operations.
 */
public class CassandraSqlQueryPutBenchmark extends CassandraQueryAbstractBenchmark {
    /** Prepared statement. */
    private PreparedStatement queryPs;

    /** {@inheritDoc} */
    @Override public void setUp(final BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        queryPs = session.prepare("SELECT * FROM Person WHERE salary >= ? AND salary <= ?")
            .setConsistencyLevel(ConsistencyLevel.ONE);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        if (rnd.nextBoolean()) {
            double salary = rnd.nextDouble() * args.range() * 1000;

            double maxSalary = salary + 1000;

            Collection<Person> persons = executeQuery(salary, maxSalary);

            for (Person p : persons)
                if (p.getSalary() < salary || p.getSalary() > maxSalary)
                    throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
                        ", person=" + p + ']');
        }
        else {
            int i = rnd.nextInt(args.range());

            put(new Person(i, "firstName" + i, "lastName" + i, i * 1000));
        }

        return true;
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
