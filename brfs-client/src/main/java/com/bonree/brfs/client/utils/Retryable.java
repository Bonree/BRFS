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

public interface Retryable<T> {
    String getDescription();

    Result<T> tryExecute();

    static interface Result<T> {
        T getResult();

        Throwable getCause();

        Retryable<T> retry();
    }

    static <T> Result<T> success(T result) {
        return retry(result, null, null);
    }

    static <T> Result<T> fail(Throwable cause) {
        return retry(null, null, cause);
    }

    static <T> Result<T> retry(Retryable<T> retryable) {
        return retry(null, retryable, null);
    }

    static <T> Result<T> retry(Retryable<T> retryable, Throwable cause) {
        return retry(null, retryable, cause);
    }

    static <T> Result<T> retry(T result, Retryable<T> retryable, Throwable cause) {
        return new Result<T>() {

            @Override
            public T getResult() {
                return result;
            }

            @Override
            public Throwable getCause() {
                return cause;
            }

            @Override
            public Retryable<T> retry() {
                return retryable;
            }
        };
    }
}
