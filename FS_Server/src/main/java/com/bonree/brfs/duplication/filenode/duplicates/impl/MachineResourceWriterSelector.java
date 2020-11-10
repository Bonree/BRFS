package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
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
    private static long preTime = 0L;
    //记录重复数值的
    private int centSize;
    private LimitServerResource limit;
    private FileNodeStorer storer;
    private long fileSize = 0;

    public MachineResourceWriterSelector(FileNodeStorer storer, LimitServerResource limit) {
        this.storer = storer;
        this.centSize = limit.getCentSize();
        this.limit = limit;
        this.fileSize = limit.getFileSize();
    }

    @Override
    public Collection<ResourceModel> filterService(Collection<ResourceModel> resourceModels, String sn) {
        // 无资源
        if (resourceModels == null || resourceModels.isEmpty()) {
            LOG.warn("no resource to selector");
            return null;
        }
        Set<ResourceModel> wins = new HashSet<>();
        long diskRemainSize;
        int numSize = this.storer == null ? 0 : this.storer.fileNodeSize();

        List<ResourceModel> washroom = new ArrayList<>();
        // 将已经满足条件的服务过滤
        for (ResourceModel wash : resourceModels) {
            diskRemainSize = wash.getFreeSize();
            if (diskRemainSize < this.limit.getRemainForceSize()) {
                LOG.warn("First: {}({}), remainsize: {}, force:{} !! will refused",
                         wash.getServerId(), wash.getHost(), diskRemainSize, this.limit.getRemainForceSize());
                continue;
            }
            washroom.add(wash);
        }
        if (washroom.isEmpty()) {
            return null;
        }
        int size = washroom.size();
        // 预测值，假设现在所有正在写的文件大小为0，并且每个磁盘节点都写入。通过现有写入的文件的数×配置的文件大小即可得单个数据节点写入数据的大小
        long writeSize = numSize * fileSize / size;
        for (ResourceModel resourceModel : washroom) {
            diskRemainSize = resourceModel.getFreeSize() - writeSize;
            if (diskRemainSize < this.limit.getRemainForceSize()) {
                LOG.warn("Second : {}({}),  remainsize: {}, force:{} !! will refused", resourceModel.getServerId(),
                         resourceModel.getHost(), diskRemainSize, this.limit.getRemainForceSize());
                continue;
            }
            if (diskRemainSize < this.limit.getRemainWarnSize()) {
                LOG.warn("sn: {}({}), remainsize: {}, force:{} !! will full", resourceModel.getServerId(),
                         resourceModel.getHost(), diskRemainSize, this.limit.getRemainForceSize());
            }
            wins.add(resourceModel);
        }
        return wins;
    }

    @Override
    public Collection<ResourceModel> selector(Collection<ResourceModel> resources, String sn, int num) {
        if (resources == null || resources.isEmpty()) {
            return null;
        }
        // 如果可选服务少于需要的，发送报警邮件
        int resourceSize = resources.size();
        boolean lessFlag = resourceSize < num;
        long currentTime = System.currentTimeMillis();
        if (lessFlag) {
            // 控制邮件发送的间隔，减少不必要的
            if (currentTime - preTime > 360000) {
                preTime = currentTime;
                sendSelectEmail(resources, num);
            }
            return resources;
        }
        // 转换为Map
        Map<String, ResourceModel> map = convertResourceMap(resources);
        // 转换为权重值
        List<Pair<String, Integer>> intValues = covertValues(resources, centSize);
        List<ResourceModel> wins = selectNode(map, intValues, num);
        int winSize = wins.size();
        // 若根据ip分配的个数满足要求，则返回该集合，不满足则看是否为
        if (winSize == num) {
            return wins;
        }
        LOG.warn("will select service in same ip !!!");
        Set<String> sids = selectWins(wins);
        // 二次选择服务
        int ssize = resourceSize > num ? num - winSize : resourceSize - winSize;
        Collection<ResourceModel> resourceModels = selectRandom(map, sids, intValues, ssize);
        wins.addAll(resourceModels);
        // 若依旧不满足则发送邮件
        if (wins.size() < num && currentTime - preTime > 360000) {
            sendSelectEmail(wins, num);
        }
        return wins;

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
     *
     * @param map
     * @param sids
     * @param intValues
     * @param num
     *
     * @return
     */
    public Collection<ResourceModel> selectRandom(Map<String, ResourceModel> map, Set<String> sids,
                                                  List<Pair<String, Integer>> intValues, int num) {
        List<ResourceModel> resourceModels = new ArrayList<>();
        String key;
        ResourceModel tmp;
        //ip选中优先选择
        int size = map.size();
        // 按资源选择
        Random random = new Random();
        while (resourceModels.size() != num && resourceModels.size() != size && sids.size() != size) {
            key = WeightRandomPattern.getWeightRandom(intValues, random, sids);
            tmp = map.get(key);
            resourceModels.add(tmp);
            sids.add(tmp.getServerId());
        }
        return resourceModels;
    }

    /**
     * 获取已选择服务的services
     *
     * @param wins
     *
     * @return
     */
    public Set<String> selectWins(List<ResourceModel> wins) {
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
     * 服务选择
     *
     * @param intValues
     * @param num
     *
     * @return
     */
    public List<ResourceModel> selectNode(Map<String, ResourceModel> map,
                                          List<Pair<String, Integer>> intValues, int num) {
        List<ResourceModel> resourceModels = new ArrayList<>();
        String key;
        ResourceModel tmp;
        //ip选中优先选择
        List<String> uneedServices = new ArrayList<>();
        int size = map.size();
        // 按资源选择
        while (resourceModels.size() != num && resourceModels.size() != size && uneedServices.size() != size) {
            key = WeightRandomPattern.getWeightRandom(intValues, new Random(), uneedServices);
            tmp = map.get(key);
            uneedServices.add(tmp.getServerId());
        }
        return resourceModels;
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
