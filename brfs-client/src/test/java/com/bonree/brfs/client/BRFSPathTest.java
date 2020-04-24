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

import java.nio.file.Paths;

public class BRFSPathTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
        System.out.println(Paths.get("abd"));
        System.out.println(BRFSPath.get("abd"));

        System.out.println("########################");

        System.out.println(Paths.get("/", "a/b"));
        System.out.println(BRFSPath.get("/", "a/b"));

        System.out.println("########################");

        System.out.println(Paths.get("/c", "/a/b/"));
        System.out.println(BRFSPath.get("/c", "/a/b/"));

        System.out.println("########################");

        System.out.println(Paths.get("/c", "/a / b/"));
        System.out.println(BRFSPath.get("/c", "/a / b/"));

        System.out.println("########################");

        System.out.println(Paths.get("", "/a / b/"));
        System.out.println(BRFSPath.get("", "/a / b/"));

        System.out.println("########################");

        System.out.println(Paths.get("", "a / b/"));
        System.out.println(BRFSPath.get("", "a / b/"));

        System.out.println("########################");

        System.out.println(Paths.get("/", ""));
        System.out.println(BRFSPath.get("/", ""));

        System.out.println("########################");

        System.out.println(Paths.get("", ""));
        System.out.println(BRFSPath.get("", ""));
    }

}
