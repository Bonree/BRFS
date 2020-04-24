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

package com.bonree.brfs.client.json;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class JsonCodec {
    private final ObjectMapper objectMapper;

    public JsonCodec(ObjectMapper objectMapper) {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    public <T> T fromJson(String json, Class<T> cls) throws IOException {
        return objectMapper.readerFor(cls).readValue(json);
    }

    public <T> T fromJson(String json, TypeReference<T> type) throws IOException {
        return objectMapper.readerFor(type).readValue(json);
    }

    public <T> T fromJsonBytes(byte[] bytes, Class<T> cls) throws IOException {
        return objectMapper.readerFor(cls).readValue(bytes);
    }

    public <T> T fromJsonBytes(byte[] bytes, TypeReference<T> type) throws IOException {
        return objectMapper.readerFor(type).readValue(bytes);
    }

    public <T> String toJson(T obj) throws JsonProcessingException {
        return objectMapper.writerFor(obj.getClass()).writeValueAsString(obj);
    }

    public <T> String toJson(T obj, TypeReference<T> type) throws JsonProcessingException {
        return objectMapper.writerFor(type).writeValueAsString(obj);
    }

    public <T> byte[] toJsonBytes(T obj) throws JsonProcessingException {
        return objectMapper.writerFor(obj.getClass()).writeValueAsBytes(obj);
    }

    public <T> byte[] toJsonBytes(T obj, TypeReference<T> type) throws JsonProcessingException {
        return objectMapper.writerFor(type).writeValueAsBytes(obj);
    }
}
