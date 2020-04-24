/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bonree.brfs.client.utils;

import static java.util.Objects.requireNonNull;

import com.bonree.brfs.client.NoNodeException;
import com.bonree.brfs.client.utils.Retrys.MultiObjectRetryable;
import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class URIRetryable<T> extends MultiObjectRetryable<T, URI> {
    private final AtomicBoolean retryable = new AtomicBoolean(true);

    private final Task<T> task;

    public URIRetryable(String description, Iterable<URI> iterable, Task<T> task) {
        this(description, iterable.iterator(), task);
    }

    public URIRetryable(String description, Iterator<URI> iter, Task<T> task) {
        super(description, iter);
        this.task = requireNonNull(task, "task is null");
    }

    @Override
    protected Throwable noMoreObjectError() {
        return new NoNodeException("no node is found for task[%s]!", getDescription());
    }

    @Override
    protected boolean continueRetry() {
        return retryable.get();
    }

    @Override
    protected T execute(URI uri) throws Exception {
        TaskResult<T> result = task.execute(uri);
        if (result.isCompleted()) {
            retryable.set(false);
            if (result.getCause() != null) {
                throw result.getCause();
            }

            return result.getResult();
        }

        throw requireNonNull(result.getCause());
    }

    public static interface Task<T> {
        TaskResult<T> execute(URI uri);
    }

    public static interface TaskResult<T> {
        boolean isCompleted();

        Exception getCause();

        T getResult();

        static <T> TaskResult<T> success(T result) {
            return new TaskResult<T>() {

                @Override
                public boolean isCompleted() {
                    return true;
                }

                @Override
                public Exception getCause() {
                    return null;
                }

                @Override
                public T getResult() {
                    return result;
                }
            };
        }

        static <T> TaskResult<T> fail(Exception cause) {
            return new TaskResult<T>() {

                @Override
                public boolean isCompleted() {
                    return true;
                }

                @Override
                public Exception getCause() {
                    return cause;
                }

                @Override
                public T getResult() {
                    return null;
                }
            };
        }

        static <T> TaskResult<T> retry(Exception cause) {
            return new TaskResult<T>() {

                @Override
                public boolean isCompleted() {
                    return false;
                }

                @Override
                public Exception getCause() {
                    return cause;
                }

                @Override
                public T getResult() {
                    return null;
                }
            };
        }
    }
}

