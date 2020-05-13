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

package com.bonree.brfs.common.zookeeper.curator;

import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.utils.StringUtils;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.inject.Singleton;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.framework.imps.GzipCompressionProvider;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.shaded.com.google.common.base.Strings;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorModule implements Module {
    private static final Logger log = LoggerFactory.getLogger(CuratorModule.class);

    private static final int BASE_SLEEP_TIME_MS = 1000;

    private static final int MAX_SLEEP_TIME_MS = 45000;

    private static final int MAX_RETRIES = 30;

    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "zookeeper", CuratorConfig.class);
    }

    @Provides
    @Singleton
    public CuratorFramework getCurator(CuratorConfig config, Lifecycle lifecycle) {
        final Builder builder = CuratorFrameworkFactory.builder();
        if (!Strings.isNullOrEmpty(config.getZkUser()) && !Strings.isNullOrEmpty(config.getZkPasswd())) {
            builder.authorization(
                config.getAuthScheme(),
                StringUtils.format("%s:%s", config.getZkUser(), config.getZkPasswd()).getBytes(StandardCharsets.UTF_8)
            );
        }

        if (config.isEnableCompression()) {
            builder.compressionProvider(new GzipCompressionProvider());
        }

        final CuratorFramework framework = builder
            .ensembleProvider(new FixedEnsembleProvider(config.getAddresses()))
            .sessionTimeoutMs(config.getZkSessionTimeoutMs())
            .retryPolicy(new BoundedExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_SLEEP_TIME_MS, MAX_RETRIES))
            .aclProvider(config.isEnableAcl() ? new SecuredACLProvider() : new DefaultACLProvider())
            .build();

        framework.getUnhandledErrorListenable().addListener((message, e) -> {
            log.error("Unhandled error in Curator Framework", e);
            try {
                lifecycle.stop();
            } catch (Throwable t) {
                log.warn("Exception when stopping druid lifecycle", t);
            }
        });

        lifecycle.addLifeCycleObject(new Lifecycle.LifeCycleObject() {

            @Override
            public void start() throws Exception {
                log.info("start curator framework");
                //framework.start();
            }

            @Override
            public void stop() {
                log.info("stop curator framework");
                framework.close();
            }
        }, Lifecycle.Stage.INIT);

        try {
            // the design of first release requires that connection to
            // zookeeper should be established before building other instances
            framework.start();
            framework.blockUntilConnected();
        } catch (InterruptedException e1) {
            throw new RuntimeException(e1);
        }

        return framework;
    }

    static class SecuredACLProvider implements ACLProvider {
        @Override
        public List<ACL> getDefaultAcl() {
            return ZooDefs.Ids.CREATOR_ALL_ACL;
        }

        @Override
        public List<ACL> getAclForPath(String path) {
            return ZooDefs.Ids.CREATOR_ALL_ACL;
        }
    }
}
