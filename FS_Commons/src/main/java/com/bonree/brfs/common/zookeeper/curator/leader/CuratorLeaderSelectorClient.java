package com.bonree.brfs.common.zookeeper.curator.leader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.Configuration;

public class CuratorLeaderSelectorClient {

    private final static Logger LOG = LoggerFactory.getLogger(CuratorLeaderSelectorClient.class);

    private volatile static CuratorLeaderSelectorClient selector = null;

    private Map<String, List<LeaderSelector>> selectorMap;

    private CuratorClient client = null;

    private CuratorLeaderSelectorClient(String zkUrl) {
        client = CuratorClient.getClientInstance(zkUrl);
        selectorMap = new ConcurrentHashMap<String, List<LeaderSelector>>();
    }

    public static CuratorLeaderSelectorClient getLeaderSelectorInstance(String zkUrl) {

        LOG.info("create CuratorLeaderSelector...");
        if (selector == null) {
            synchronized (Configuration.class) {
                if (selector == null) {
                    selector = new CuratorLeaderSelectorClient(zkUrl);
                }
            }
        }
        return selector;
    }

    /** 概述：添加一个leaderSelectorLinstener
     * @param path 监听路径
     * @param listener 监听器
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public synchronized void addSelector(String path, LeaderSelectorListener listener) {
        List<LeaderSelector> leaderSelectors = selectorMap.get(path);
        if (leaderSelectors == null) {
            leaderSelectors = new ArrayList<LeaderSelector>();
        }
        LeaderSelector leaderSelector = new LeaderSelector(client.getInnerClient(), path, listener);
        leaderSelector.autoRequeue();
        leaderSelectors.add(leaderSelector);
        leaderSelector.start();
        selectorMap.put(path, leaderSelectors);

    }

    /** 概述：添加一个leaderSelectorLinstener
     * @param path 监听路径
     * @param id 为监听器指定一个id，如果不指定，则为空
     * @param listener 监听器
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public synchronized void addSelector(String path, String id, LeaderSelectorListener listener) {
        List<LeaderSelector> leaderSelectors = selectorMap.get(path);
        if (leaderSelectors == null) {
            leaderSelectors = new ArrayList<LeaderSelector>();
        }
        LeaderSelector leaderSelector = new LeaderSelector(client.getInnerClient(), path, listener);
        leaderSelector.setId(id);
        leaderSelector.autoRequeue();
        leaderSelectors.add(leaderSelector);
        leaderSelector.start();
        selectorMap.put(path, leaderSelectors);

    }

    /** 概述：移除path下的所有leader监听器
     * @param path 监听的path
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void removeAllSelector(String path) {
        List<LeaderSelector> leaderSelectors = selectorMap.get(path);
        if (leaderSelectors != null) {

            for (LeaderSelector selector : leaderSelectors) {
                selector.close();
            }
            leaderSelectors.clear();
            selectorMap.remove(path);
        }
    }

    /** 概述：不能跨client移除，只能移除本client中的selector
     * @param path
     * @param id
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void removeSelectorById(String path, String id) {
        List<LeaderSelector> leaderSelectors = selectorMap.get(path);
        if (leaderSelectors != null) {

            Iterator<LeaderSelector> it = leaderSelectors.iterator();
            while (it.hasNext()) {
                LeaderSelector selector = it.next();
                if (StringUtils.equals(selector.getId(), id)) {
                    selector.close();
                    it.remove();
                }
            }
            if (leaderSelectors.size() == 0) {
                selectorMap.remove(path);
            }
        }
    }

    /** 概述：获取本path下，有几个leader监听器
    * @param path 监听的path
    * @return
    * @user <a href=mailto:weizheng@bonree.com>魏征</a>
    */
    public int getSelectorListeners(String path) {
        List<LeaderSelector> leaderSelectors = selectorMap.get(path);
        if (leaderSelectors != null) {
            if (leaderSelectors.size() > 0) {
                try {
                    return leaderSelectors.get(0).getParticipants().size();
                } catch (Exception e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            }
        }
        return 0;
    }

    /** 概述：获取leader的ID
     * @param path 监听路径
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String getLeaderId(String path) {
        List<LeaderSelector> leaderSelectors = selectorMap.get(path);
        if (leaderSelectors != null && leaderSelectors.size() > 0) {
            try {
                return leaderSelectors.get(0).getLeader().getId();
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
        return null;
    }

}
