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

import static com.bonree.brfs.client.utils.Strings.format;
import static java.util.Objects.requireNonNull;

import com.bonree.brfs.client.ClientException;
import com.bonree.brfs.client.FidException;
import com.bonree.brfs.client.FidExpiredException;
import com.bonree.brfs.client.utils.Retryable.Result;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Retrys {

    public static <T> T execute(Retryable<T> retryable) {
        Retryable<T> next = requireNonNull(retryable);
        Result<T> result = null;
        while (next != null) {
            try {
                result = next.tryExecute();
                if (result == null) {
                    throw new NullPointerException(format("result is null when try execute [%s]", next.getDescription()));
                }
                if (result.getCause() != null && result.getCause().getCause() instanceof FidExpiredException) {
                    throw result.getCause().getCause();
                }
                next = result.retry();
            } catch (FidExpiredException fd) {
                throw new FidException("error about fid");
            } catch (Throwable t) {
                throw new ClientException(t, "error when try execute [%s]", next.getDescription());
            }
        }

        if (result.getCause() != null) {
            throw new ClientException(result.getCause(), "Error when execute [%s]", retryable.getDescription());
        }

        return result.getResult();
    }

    public abstract static class MultiObjectRetryable<T, E> implements Retryable<T> {
        private static final Logger log = LoggerFactory.getLogger(MultiObjectRetryable.class);

        private final String description;
        private final Iterator<E> iter;

        private final List<String> triedItems = new ArrayList<>();

        private Throwable cause;

        protected MultiObjectRetryable(String description, Iterator<E> iter) {
            this.description = description;
            this.iter = iter;
        }

        protected abstract Throwable noMoreObjectError();

        protected abstract T execute(E obj) throws Exception;

        protected boolean continueRetry() {
            return true;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public Result<T> tryExecute() {
            if (!iter.hasNext()) {
                return Retryable.fail(cause != null ? new RetryException(triedItems, cause) : noMoreObjectError());
            }

            E element = iter.next();
            try {
                triedItems.add(element.toString());
                T result = execute(element);
                return Retryable.success(result);
            } catch (Exception e) {
                log.warn("{} fail to execute on [{}]", description, element);
                cause = e;

                if (!continueRetry()) {
                    return Retryable.fail(new RetryException(triedItems, e));
                }
            }

            return Retryable.retry(this, cause);
        }

    }

    public static class RetryException extends Exception {

        public RetryException(List<String> retriedElements, Throwable cause) {
            super(Strings.format("Failed after tried with %s", retriedElements), cause);
        }
    }
}
