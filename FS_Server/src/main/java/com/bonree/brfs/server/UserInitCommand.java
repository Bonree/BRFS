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

package com.bonree.brfs.server;

import com.bonree.brfs.authentication.SimpleAuthentication;
import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import io.airlift.airline.Command;
import java.util.Scanner;
import org.apache.commons.lang3.StringUtils;

@Command(
    name = "init",
    description = "initialize user and password"
)
public class UserInitCommand implements Runnable {

    @Override
    public void run() {

        try {
            String zkAddresses = Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
            CuratorClient client = CuratorClient.getClientInstance(zkAddresses);
            ZookeeperPaths zookeeperPaths = ZookeeperPaths
                .create(Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_CLUSTER_NAME), client.getInnerClient());
            CuratorCacheFactory.init(client.getInnerClient());
            String passwd = null;
            Scanner sc = new Scanner(System.in);
            Thread.sleep(500);
            while (true) {
                System.out.println("please input root user's password:");
                passwd = sc.nextLine();
                if (StringUtils.isEmpty(passwd)) {
                    System.out.println("password is empty!!");
                } else if (passwd.length() < 5) {
                    System.out.println("password less 5 size!!");
                } else {
                    System.out.println("password setup successfully!!");
                    break;
                }
            }

            SimpleAuthentication authentication =
                SimpleAuthentication.getAuthInstance(zookeeperPaths.getBaseLocksPath(), client.getInnerClient());
            authentication.init(zookeeperPaths.getBaseUserPath());
            UserModel user = new UserModel("root", passwd, (byte) 0);
            authentication.createUser(user);
            System.out.println("init server successfully!!");
            sc.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            System.exit(0);
        }

    }

}
