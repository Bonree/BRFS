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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
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
        ObjectMapper objectMapper = new ObjectMapper(new JsonFactory());

        // ignore unknown fields (for backwards compatibility)
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // do not allow converting a float to an integer
        objectMapper.disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);

        // use ISO dates
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Skip fields that are null or absent (Optional) when serializing objects.
        // This only applies to mapped object fields, not containers like Map or List.
        objectMapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_ABSENT, JsonInclude.Include.ALWAYS));

        // disable auto detection of json properties... all properties must be explicit
        objectMapper.disable(MapperFeature.AUTO_DETECT_CREATORS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_FIELDS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_SETTERS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_GETTERS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        objectMapper.disable(MapperFeature.USE_GETTERS_AS_SETTERS);
        objectMapper.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
        objectMapper.disable(MapperFeature.INFER_PROPERTY_MUTATORS);
        objectMapper.disable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS);

        return objectMapper;
    }
}
