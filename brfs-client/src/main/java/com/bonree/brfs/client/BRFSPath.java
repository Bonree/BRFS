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

import com.google.common.base.Strings;

public class BRFSPath {
    private static final char PATH_SEPARATOR_CHAR = '/';
    private static final String PATH_SEPARATOR = String.valueOf(PATH_SEPARATOR_CHAR);

    private final String path;

    private BRFSPath(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (path == obj) {
            return true;
        }

        if (!(obj instanceof BRFSPath)) {
            return false;
        }

        BRFSPath oth = (BRFSPath) obj;
        return path.equals(oth.path);
    }

    @Override
    public String toString() {
        return path;
    }

    public static BRFSPath get(String root, String... subPath) {
        StringBuilder pathBuilder = new StringBuilder();
        if (root.startsWith(PATH_SEPARATOR)) {
            pathBuilder.append(PATH_SEPARATOR);
        }

        appendPath(pathBuilder, trimPath(root));
        for (String path : subPath) {
            appendPath(pathBuilder, trimPath(path));
        }

        if (pathBuilder.length() > 1) {
            pathBuilder.deleteCharAt(pathBuilder.length() - 1);
        }

        return new BRFSPath(pathBuilder.toString());
    }

    private static void appendPath(StringBuilder builder, String path) {
        if (!Strings.isNullOrEmpty(path)) {
            builder.append(path).append(PATH_SEPARATOR);
        }
    }

    private static String trimPath(String value) {
        int len = value.length();
        int st = 0;

        while ((st < len) && (value.charAt(st) <= ' ' || value.charAt(st) == PATH_SEPARATOR_CHAR)) {
            st++;
        }
        while ((st < len) && (value.charAt(len - 1) <= ' ' || value.charAt(len - 1) == PATH_SEPARATOR_CHAR)) {
            len--;
        }

        return ((st > 0) || (len < value.length())) ? value.substring(st, len) : value;
    }
}
