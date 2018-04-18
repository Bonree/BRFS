//package com.bonree.brfs.client.loadbalance;
//
//import java.util.ArrayList;
//import java.util.List;
//
//public class SecondServerIDRoute {
//
//    public SecondServerIDRoute() {
//        // TODO Auto-generated constructor stub
//    }
//
//    public String findRoute(String secondServerId) {
//        
//        
//    }
//
//    // 检测文件是否查询成功
//    public void checkAllFile() {
//        System.out.println("开始检查所有的文件!");
//        boolean result = true;
//        for (String fileName : allFileList) {
//
//            String[] metaArr = fileName.split(NAME_SEPARATOR);
//            String namePart = metaArr[0]; // 用于特征hash
//
//            for (int i = 1; i < metaArr.length; i++) {
//                String checkServer = metaArr[i];
//                // 存放所有存活的server，假设所有的server都是存活的
//                List<String> aliveServerIdList = new ArrayList<>();
//                for (int j = 1; j < metaArr.length; j++) {
//                    aliveServerIdList.add(metaArr[j]);
//                }
//                // 多副本才计算副本迁移路由
//                if (aliveServerIdList.size() > 1) {
//                    // 如果checkServer还在deadedServerIdList中，则需要继续更新修改aliveServerIdList
//                    ServerModel selectServerModel = null;
//                    List<ServerModel> recoverableServerList = null;
//                    List<String> exceptionServerIds = null;
//                    List<ServerModel> selectableServerList = null;
//                    while (deadedServerIdList.contains(checkServer)) {
//                        for (String deadServer : deadedServerIdList) {
//                            if (aliveServerIdList.contains(deadServer)) {
//                                int pot = aliveServerIdList.indexOf(deadServer);
//                                recoverableServerList = getRecoverRoleList(deadServer);
//                                exceptionServerIds = new ArrayList<>();
//                                exceptionServerIds.addAll(aliveServerIdList);
//                                exceptionServerIds.remove(deadServer);
//                                selectableServerList = getSelectedList(recoverableServerList, exceptionServerIds);
//                                int index = hashFileName(namePart, selectableServerList.size());
//                                selectServerModel = selectableServerList.get(index);
//                                aliveServerIdList.set(pot, selectServerModel.getServerID());
//                                if (checkServer.equals(deadServer)) {
//                                    checkServer = selectServerModel.getServerID();
//                                }a
//                            }
//                        }
//                    }
//
//                    for (ServerModel sm : smList) {
//                        if (sm.getServerID().equals(checkServer)) {
//                            if (!sm.getFileList(i).contains(fileName)) {
//                                result = false;
//                            }
//                        }
//                    }
//                } else if (aliveServerIdList.size() == 1) {
//                    System.out.println("单副本，无副本迁移");
//                }
//            }
//        }
//        System.out.println("全部匹配成功：" + result);
//    }
//
//}
