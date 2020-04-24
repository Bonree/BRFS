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

package com.bonree.brfs.client;

import com.bonree.brfs.client.utils.Retrys;
import com.bonree.brfs.client.utils.URIRetryable;
import com.bonree.brfs.client.utils.URIRetryable.TaskResult;
import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

public class RetrysTest {

    public static void main(String[] args) {
        AtomicInteger i = new AtomicInteger();
        String r = Retrys.execute(new URIRetryable<String>(
            "test",
            ImmutableList.of(
                URI.create("localhost:10"),
                URI.create("localhost:11"),
                URI.create("localhost:12")),
            uri -> {
                System.out.println("uri : " + uri);

                if (i.get() == 1) {
                    return TaskResult.fail(new Exception("final cause:" + uri));
                }

                if (i.getAndIncrement() < 3) {
                    System.out.println("ERROR : " + uri);
                    return TaskResult.retry(new Exception("cause:" + uri));
                }

                return TaskResult.success("YES");
            }));

        System.out.println("result : " + r);
    }
}
