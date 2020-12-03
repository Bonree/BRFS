package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.duplication.datastream.file.DuplicateNodeChecker;
import com.bonree.brfs.duplication.filenode.duplicates.ServiceSelector;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.resource.vo.LimitServerResource;
import com.bonree.brfs.resource.vo.ResourceModel;
import com.bonree.mail.worker.MailWorker;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务写数据服务选择策略 按磁盘剩余量选择服务，选择服务只会选择不同ip的，可能出现选出的节点数与需求的长度不一致
 */
public class MachineResourceWriterSelector implements ServiceSelector {
    private static final Logger LOG = LoggerFactory.getLogger(MachineResourceWriterSelector.class);
    //记录少于服务时
    private static long lastSendWarningEmailTime = 0L;
    //记录重复数值的
    private int centSize;
    private LimitServerResource limit;
    private DuplicateNodeChecker checker;
    private long fileSize = 0;

    public MachineResourceWriterSelector(LimitServerResource limit, DuplicateNodeChecker checker) {
        this.centSize = limit.getCentSize();
        this.limit = limit;
        this.checker = checker;
        this.fileSize = limit.getFileSize();
    }

    /**
     * 先排除不可用(如磁盘紧张)的节点
     * @param resources
     */
    private void filterService(Collection<ResourceModel> resources) {
        // 无资源
        if (resources == null || resources.isEmpty()) {
            LOG.warn("no resource to selector");
            return;
        }
        long diskRemainSize;
        // 将剩余容量不多的节点过滤掉
        for (ResourceModel resource : resources) {
            diskRemainSize = resource.getFreeSize();
            if (diskRemainSize < this.limit.getRemainForceSize()) {
                LOG.warn("node: {}({}) now remain size is : {}, it is lower than config force size:{} !!",
                         resource.getServerId(),
                         resource.getHost(),
                         diskRemainSize,
                         this.limit.getRemainForceSize());
                resources.remove(resource);
            }
            if (diskRemainSize < this.limit.getRemainWarnSize()) {
                LOG.warn("node: {}({}), remain size is: {}, the warn size is:{} !! ",
                         resource.getServerId(),
                         resource.getHost(),
                         diskRemainSize,
                         this.limit.getRemainWarnSize());
            }
        }
    }

    @Override
    public Collection<ResourceModel> selector(Collection<ResourceModel> resources, int n) {
        // 按过滤策略来过滤节点
        filterService(resources);
        LOG.info("service num after filter: [{}]", resources.size());
        if (resources == null || resources.isEmpty()) {
            LOG.error("[{}] there is not available node to selector !!!");
            resources = new HashSet<>();
        }

        // 如果可选服务少于需要的，发送报警邮件
        long currentTime = System.currentTimeMillis();
        if (resources.size() < n) {
            // 控制邮件发送的间隔，减少不必要的
            if (currentTime - lastSendWarningEmailTime > 360000) {
                lastSendWarningEmailTime = currentTime;
                sendSelectEmail(resources, n);
            }
            return resources;
        }
        Collection<ResourceModel> result;
        // 到此,可选节点数大于等于n
        result = selectNodes(resources, n);
        if (result.size() == n) {
            return result;
        }
        LOG.warn("resource selector cannot select all nodes, expect size[{}], actual size[{}] !!!", n, result.size());
        // 随机选择
        int patchSize = resources.size() > n ? n - result.size() : resources.size() - result.size();
        // 此时传入的resources是已经删除掉已选元素的集合
        Collection<ResourceModel> patches = selectRandom(resources, patchSize);
        result.addAll(patches);
        // 若依旧不满足则发送邮件
        if (result.size() < n && currentTime - lastSendWarningEmailTime > 360000) {
            sendSelectEmail(result, n);
        }
        return result;
    }

    /**
     * 发送选择邮件
     *
     * @param resourceModels
     * @param num
     */
    public void sendSelectEmail(Collection<ResourceModel> resourceModels, int num) {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append(" 写入可供选择的服务少于需要的!! 可用服务 ")
                      .append(resourceModels.size()).append(", 需要 ")
                      .append(num).append("(文件分布见上下文表格)");
        Map<String, String> map = new HashMap<>();
        String part;
        String key;
        for (ResourceModel resource : resourceModels) {
            key = resource.getServerId() + "(" + resource.getHost() + ")";
            part = resource.getFreeSize() + "B";
            map.put(key, part);
        }
        EmailPool emailPool = EmailPool.getInstance();
        MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
        builder.setModel(this.getClass().getSimpleName() + "服务选择");
        builder.setMessage(messageBuilder.toString());
        if (!map.isEmpty()) {
            builder.setVariable(map);
        }
        emailPool.sendEmail(builder);
    }

    /**
     * 随机选择
     * @param num
     *
     * @return
     */
    public Collection<ResourceModel> selectRandom(Collection<ResourceModel> resources, int num) {
        if (resources.size() <= num) {
            return resources;
        }
        ArrayList<ResourceModel> result = new ArrayList<>();
        int index = 0;
        // 按资源选择
        while (result.size() < num) {
            if (resources.size() == 0) {
                break;
            }
            ResourceModel resource = RandomPattern.random(resources);
            if (resource != null) {
                result.add(resource);
                resources.remove(resource);
            }
        }
        return result;
    }

    /**
     * 获取已选择服务的services
     *
     * @param wins
     *
     * @return
     */
    public Set<String> selectWins(Collection<ResourceModel> wins) {
        Set<String> set = new HashSet<>();
        for (ResourceModel resourceModel : wins) {
            set.add(resourceModel.getServerId());
        }
        return set;
    }

    public List<Pair<String, Integer>> covertValues(Collection<ResourceModel> resources, int centSize) {
        List<Pair<String, Double>> values = new ArrayList<>();
        Pair<String, Double> tmpResource;
        double sum;
        String server;
        for (ResourceModel resource : resources) {
            server = resource.getServerId();
            // 参数调整，disk写入io大的权重低
            sum = getWriteValue(resource);
            tmpResource = new Pair<>(server, sum);
            values.add(tmpResource);
        }
        return converDoublesToIntegers(values, centSize);
    }

    private double getWriteValue(ResourceModel resource) {
        return resource.getFreeSize();
    }

    /**
     * 选择指定个数的节点
     * 副作用: 会将已选择到的节点从resource集合中删除
     * @param num 目标数
     * @return 可能返回个数小于目标数
     */
    public Collection<ResourceModel> selectNodes(Collection<ResourceModel> resources, int num) {
        if (resources.size() <= num) {
            return resources;
        }
        ArrayList<ResourceModel> result = new ArrayList<>();
        int index = 0;
        // 按资源选择
        while (result.size() < num) {
            if (resources.size() == 0) {
                break;
            }
            ResourceModel resource = RandomPattern.randomWithWeight(resources, ResourceModel.class, centSize);
            if (resource != null) {
                result.add(resource);
                resources.remove(resource);
            }
        }
        return result;
    }

    /**
     * 转换为map
     *
     * @param resources
     *
     * @return
     */
    public Map<String, ResourceModel> convertResourceMap(Collection<ResourceModel> resources) {
        Map<String, ResourceModel> map = new HashMap<>();
        for (ResourceModel resource : resources) {
            map.put(resource.getServerId(), resource);
        }
        return map;
    }

    /**
     * 概述：计算资源比值
     *
     * @param servers
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    private List<Pair<String, Integer>> converDoublesToIntegers(final List<Pair<String, Double>> servers, int preCentSize) {
        List<Pair<String, Integer>> dents = new ArrayList<Pair<String, Integer>>();
        int value;
        double sum = 0;
        int centSize = preCentSize <= 0 ? 100 : preCentSize;
        for (Pair<String, Double> pair : servers) {
            sum += pair.getSecond();
        }
        Pair<String, Integer> tmp;
        for (Pair<String, Double> ele : servers) {
            tmp = new Pair<>();
            tmp.setFirst(ele.getFirst());
            value = (int) (ele.getSecond() / sum * centSize);
            if (value == 0) {
                value = 1;
            }
            tmp.setSecond(value);
            dents.add(tmp);
        }
        return dents;
    }

}
