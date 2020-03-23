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
package com.bonree.brfs.plugin;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import com.google.common.collect.Iterators;

public class PluginClassLoader extends URLClassLoader {
    
    private final ClassLoader brfsLoader;

    public PluginClassLoader(List<URL> urls, ClassLoader brfsLoader) {
        this(urls.toArray(new URL[urls.size()]), brfsLoader);
    }
    
    public PluginClassLoader(URL[] urls, ClassLoader brfsLoader) {
        super(urls, null);
        this.brfsLoader = brfsLoader;
    }
    
    @Override
    public Class<?> loadClass(final String name) throws ClassNotFoundException
    {
      return loadClass(name, false);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            // First, check if the class has already been loaded
            Class<?> c = findLoadedClass(name);
            
            if(c == null) {
                try {
                    // first find class from plugin class loader
                    c = findClass(name);
                } catch (ClassNotFoundException e) {
                    // if no class is found from plugin class loader,
                    // try brfs class loader
                    return brfsLoader.loadClass(name);
                }
            }
            
            if(resolve) {
                resolveClass(c);
            }
            
            return c;
        }
    }
    
    @Override
    public URL getResource(final String name)
    {
      final URL resourceFromExtension = super.getResource(name);

      if (resourceFromExtension != null) {
        return resourceFromExtension;
      } else {
        return brfsLoader.getResource(name);
      }
    }

    @Override
    public Enumeration<URL> getResources(final String name) throws IOException
    {
      final List<URL> urls = new ArrayList<>();
      Iterators.addAll(urls, Iterators.forEnumeration(super.getResources(name)));
      Iterators.addAll(urls, Iterators.forEnumeration(brfsLoader.getResources(name)));
      return Iterators.asEnumeration(urls.iterator());
    }
    
}
