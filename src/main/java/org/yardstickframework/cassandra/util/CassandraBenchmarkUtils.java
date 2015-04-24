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

package org.yardstickframework.cassandra.util;

import java.util.*;

/**
 * Utils.
 */
public class CassandraBenchmarkUtils {
    /**
     * Runs runnable in a give number of threads.
     *
     * @param r Runnable to execute.
     * @param threadNum Number of threads.
     * @param threadName Thread name pattern.
     * @return Execution errors if present, or empty collection in case of no errors.
     * @throws InterruptedException If execution was interrupted.
     */
    public static Collection<Throwable> runMultiThreaded(CassandraBenchmarkRunnable r, int threadNum, String threadName)
        throws InterruptedException {
        List<CassandraBenchmarkRunnable> runs = Collections.nCopies(threadNum, r);

        Collection<Thread> threads = new ArrayList<>();

        final Collection<Throwable> errors = Collections.synchronizedCollection(new ArrayList<Throwable>(threadNum));

        int threadIdx = 0;

        for (final CassandraBenchmarkRunnable runnable : runs) {
            final int threadIdx0 = threadIdx;

            threads.add(new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        runnable.run(threadIdx0);
                    }
                    catch (Exception e) {
                        e.printStackTrace();

                        errors.add(e);
                    }
                }
            }, threadName + threadIdx++));
        }

        for (Thread t : threads)
            t.start();

        // Wait threads finish their job.
        for (Thread t : threads)
            t.join();

        return errors;
    }

    /**
     */
    private CassandraBenchmarkUtils() {
        // No-op
    }
}
