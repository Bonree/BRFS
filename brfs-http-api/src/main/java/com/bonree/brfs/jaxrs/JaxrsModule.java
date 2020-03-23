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
package com.bonree.brfs.jaxrs;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

import java.util.Set;

import javax.inject.Inject;
import javax.servlet.Servlet;
import javax.ws.rs.core.Application;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

public class JaxrsModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.disableCircularProxies();
        
        binder.bind(Application.class).to(JaxRsApplication.class).in(Scopes.SINGLETON);
        binder.bind(Servlet.class).annotatedWith(TheServlet.class).to(ServletContainer.class);
        
        newSetBinder(binder, Object.class, JaxrsResource.class).permitDuplicates();
    }
    
    @Provides
    public static ServletContainer getContainer(ResourceConfig resourceConfig) {
        return new ServletContainer(resourceConfig);
    }
    
    @Provides
    public static ResourceConfig getResourceConfig(Application application) {
        return ResourceConfig.forApplication(application);
    }

    public static class JaxRsApplication extends Application {
        private final Set<Object> jaxRsSingletons;

        @Inject
        public JaxRsApplication(@JaxrsResource Set<Object> jaxRsSingletons) {
            this.jaxRsSingletons = ImmutableSet.copyOf(jaxRsSingletons);
        }

        @Override
        public Set<Object> getSingletons() {
            return jaxRsSingletons;
        }
    }
}
