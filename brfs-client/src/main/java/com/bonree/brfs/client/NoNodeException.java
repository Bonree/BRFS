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

import java.util.Locale;

public class NoNodeException extends Exception {
    private static final long serialVersionUID = 1L;

    public NoNodeException(String message) {
        super(message);
    }

    public NoNodeException(String format, Object... arguments) {
        super(String.format(Locale.ENGLISH, format, arguments));
    }

    public NoNodeException(Throwable cause, String message) {
        super(message, cause);
    }

    public NoNodeException(Throwable cause, String format, Object... arguments) {
        super(String.format(Locale.ENGLISH, format, arguments), cause);
    }
}