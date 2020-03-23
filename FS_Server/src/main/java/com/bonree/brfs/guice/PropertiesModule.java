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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import com.bonree.brfs.common.utils.StringUtils;
import com.google.inject.Binder;
import com.google.inject.Module;

public class PropertiesModule implements Module {
    
    private static final String SYS_PROPERTIES_FILE = "configuration.file";

    @Override
    public void configure(Binder binder) {
        Properties fileProperties = new Properties();
        Properties totalProperties = new Properties(fileProperties);
        totalProperties.putAll(System.getProperties());
        
        String configFilePath = System.getProperty(SYS_PROPERTIES_FILE);
        if(configFilePath == null) {
            throw new RuntimeException(StringUtils.format(
                    "No configuration file is specified by property[%s]",
                    SYS_PROPERTIES_FILE));
        }
        
        File configFile = new File(configFilePath);
        if (!configFile.exists()) {
            throw new RuntimeException(StringUtils.format(
                    "config file[%s] is not existed",
                    configFile));
        }
        
        if(!configFile.isFile()) {
            throw new RuntimeException(StringUtils.format(
                    "config file[%s] is not a regular file",
                    configFile));
        }
        
        try(InputStream stream = new BufferedInputStream(new FileInputStream(configFile))) {
            fileProperties.load(new InputStreamReader(stream, StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            // It's impossible to come here!
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        binder.bind(Properties.class).toInstance(totalProperties);
    }

}
