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
package com.bonree.brfs.email;

import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.plugin.BrfsModule;
import com.bonree.brfs.common.plugin.NodeType;
import com.google.inject.Binder;

public class EmailModule extends BrfsModule {

    @Override
    public void configure(NodeType nodeType, Binder binder) {
        JsonConfigProvider.bind(binder, "email", EmailConfig.class);
        
        LifecycleModule.register(binder, EmailPoolInitializer.class);
    }
    
}
