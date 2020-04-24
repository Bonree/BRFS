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

package com.bonree.brfs;

import com.bonree.brfs.common.guice.ConfigModule;
import com.bonree.brfs.common.http.HttpServer;
import com.bonree.brfs.common.http.rest.JaxrsBinder;
import com.bonree.brfs.common.http.rest.JaxrsModule;
import com.bonree.brfs.common.jackson.JsonMapper;
import com.bonree.brfs.common.jackson.JsonModule;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.netty.NettyHttpServerModule;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

public class ServerTest {

    /**
     * @param args
     *
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        Injector in = Guice.createInjector(binder -> {
            binder.install(new JaxrsModule());
            binder.install(new NettyHttpServerModule());
            binder.install(new LifecycleModule());
            binder.install(new JsonModule());
            binder.install(new ConfigModule());

            JaxrsBinder.jaxrs(binder).resource(A.class);
            JaxrsBinder.jaxrs(binder).resource(JsonMapper.class);
            JaxrsBinder.jaxrs(binder).resource(EE.class);
        });

        HttpServer server = in.getInstance(HttpServer.class);
        server.start();

        System.out.println("Server started");
    }

    @Path("/a")
    public static class A {

        @GET
        @Produces(MediaType.APPLICATION_JSON)
        public B get() {
            return new B(99, "HI");
        }

        @POST
        @Produces(MediaType.APPLICATION_JSON)
        public void getasync(@Suspended AsyncResponse r) {
            r.resume(new B(994, "HI"));
        }
    }

    public static class B {
        private final int anInt;
        private final String st;

        public B(@JsonProperty("aaa") int anInt, @JsonProperty("bbb") String st) {
            this.anInt = anInt;
            this.st = st;
        }

        @JsonProperty("aaa")
        public int getAnInt() {
            return anInt;
        }

        @JsonProperty("bbb")
        public String getSt() {
            return st;
        }

        @Override
        public String toString() {
            return new StringBuilder()
                .append("{")
                .append("ddda : ").append(anInt).append(",")
                .append("dddb : ").append(st)
                .append("}")
                .toString();
        }
    }

    @Provider
    public static class BProvider implements MessageBodyWriter<B> {

        @Override
        public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
            return type == B.class;
        }

        @Override
        public void writeTo(B t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
                            MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
            throws IOException, WebApplicationException {
            System.out.println("############serialize B");
            entityStream.write(t.toString().getBytes(StandardCharsets.UTF_8));
        }

    }

    @Provider
    public static class EE implements ExceptionMapper<RuntimeException> {

        @Override
        public Response toResponse(RuntimeException exception) {
            System.out.println("HERE");
            return Response.serverError().entity(Throwables.getStackTraceAsString(exception)).build();
        }

    }
}
