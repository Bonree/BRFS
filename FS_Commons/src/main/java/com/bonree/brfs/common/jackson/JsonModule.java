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
package com.bonree.brfs.common.jackson;

import javax.inject.Singleton;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;

public class JsonModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(ObjectMapper.class).to(Key.get(ObjectMapper.class, Json.class));
    }

    @Provides
    @Json
    @Singleton
    public ObjectMapper jsonMapper() {
        return new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(MapperFeature.AUTO_DETECT_GETTERS, false)
                // See https://github.com/FasterXML/jackson-databind/issues/170
                // configure(MapperFeature.AUTO_DETECT_CREATORS, false);
                .configure(MapperFeature.AUTO_DETECT_FIELDS, false)
                .configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false)
                .configure(MapperFeature.AUTO_DETECT_SETTERS, false)
                .configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false)
                .configure(SerializationFeature.INDENT_OUTPUT, false)
                .configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, false);
    }
}
