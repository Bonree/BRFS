/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月18日 18:04:17
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

package com.bonree.brfs.common.rebalance.route;

import java.util.*;

public abstract class AbstractNormalRoute implements LocateRouteServerInterface{
    /**
     * 文件名生成数值 V1版本使用的。V2兼容
     *
     * @param fileName
     * @param size
     * @return
     */
    protected int hashFileName(String fileName, int size) {
        int nameSum = sumName(fileName);
        int matchSm = nameSum % size;
        return matchSm;
    }

    /**
     * 根据文件名生成code
     * @param name
     * @return
     */
    protected int sumName(String name) {
        int sum = 0;
        for (int i = 0; i < name.length(); i++) {
            sum = sum + name.charAt(i);
        }
        return sum;
    }

    /**
     * 过滤
     * @param services
     * @return
     */
    protected List<String> filterService(Collection<String> newSecondIDs, Collection<String> services){
        List<String> selectors = new ArrayList<>();
        // 1.过滤掉已经使用的service
        if(services!=null&& !services.isEmpty()){
            for(String ele : newSecondIDs){
                if(services.contains(ele)){
                    continue;
                }
                selectors.add(ele);
            }
        }else {
            selectors.addAll(newSecondIDs);
        }
        // 2.判断集合是否为空，为空，则解析失败。
        // todo 1 定义无服务 的异常内容
        if(selectors.isEmpty()){
            throw new IllegalArgumentException("errror");
        }
        // 3.对select 服务进行排序。
        Collections.sort(selectors, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        return selectors;
    }
}
