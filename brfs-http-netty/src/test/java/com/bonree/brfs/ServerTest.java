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

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import com.bonree.brfs.http.HttpServer;
import com.bonree.brfs.jaxrs.JaxrsBinder;
import com.bonree.brfs.jaxrs.JaxrsModule;
import com.bonree.brfs.netty.NettyHttpServerModule;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class ServerTest {

    /**
     * @param args
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws InterruptedException {
        Injector in = Guice.createInjector(binder -> {
            binder.install(new JaxrsModule());
            binder.install(new NettyHttpServerModule());
            
            JaxrsBinder.jaxrs(binder).resource(A.class);
            JaxrsBinder.jaxrs(binder).resource(BProvider.class);
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
//            return new B(99, "HI");
//            return null;
            throw new RuntimeException("NONONO");
        }
        
        @GET
        @Path("r")
        public Response getR() throws IOException {
//            return Response.ok().entity("hahah").build();
            throw new RuntimeException("III");
        }
    }
    
    public static class B {
        private int a;
        private String b;
        
        public B(int a, String b) {
            this.a = a;
            this.b = b;
        }
        
        public String toJson() {
            return new StringBuilder()
                    .append("{")
                    .append("a : ").append(a).append(",")
                    .append("b : ").append(b)
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
            entityStream.write(t.toJson().getBytes(StandardCharsets.UTF_8));
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
