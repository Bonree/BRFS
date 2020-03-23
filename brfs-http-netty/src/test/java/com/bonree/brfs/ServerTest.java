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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import com.bonree.brfs.http.HttpServer;
import com.bonree.brfs.jaxrs.JaxrsBinder;
import com.bonree.brfs.jaxrs.JaxrsModule;
import com.bonree.brfs.netty.NettyHttpServerModule;
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
        });
        
        HttpServer server = in.getInstance(HttpServer.class);
        server.start();
        
        System.out.println("Server started");
    }
    
    @Path("/a")
    public static class A {
        
        @GET
        public Response get() {
            return Response.ok("hello netty\n").build();
        }
    }

}
