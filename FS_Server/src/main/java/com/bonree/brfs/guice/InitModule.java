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

package com.bonree.brfs.guice;

import com.bonree.brfs.common.guice.JsonConfigurator;
import com.bonree.brfs.common.jackson.Json;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import java.util.Properties;
import javax.inject.Inject;
import javax.validation.Validator;

public class InitModule implements Module {
    private final Properties properties;
    private final Validator validator;
    private final ObjectMapper jsonMapper;

    @Inject
    public InitModule(
        Properties properties,
        Validator validator,
        @Json ObjectMapper jsonMapper) {
        this.properties = properties;
        this.validator = validator;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(Properties.class).toInstance(properties);
        binder.bind(Validator.class).toInstance(validator);
        binder.bind(ObjectMapper.class).to(Key.get(ObjectMapper.class, Json.class));
        binder.bind(JsonConfigurator.class);
    }

    @Provides
    @Json
    public ObjectMapper jsonMapper() {
        return jsonMapper;
    }
}
