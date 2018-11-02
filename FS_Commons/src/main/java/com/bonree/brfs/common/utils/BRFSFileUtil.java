package com.bonree.brfs.common.utils;
import com.bonree.brfs.common.files.FileFilterInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/***
 * BRFS文件操作工具类，
 *
 */
public class BRFSFileUtil{
    private final static Logger LOG = LoggerFactory.getLogger(BRFSFileUtil.class);

    public static List<BRFSPath> scanDirs(String rootPath){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size()-1);
    }

    public static List<BRFSPath> scanDirs(String rootPath, String storageRegion){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size()-1, storageRegion);
    }
    public static List<BRFSPath> scanDirs(String rootPath, String storageRegion, String index){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size()-1, storageRegion, index);
    }

    public static List<BRFSPath> scanDirs(String rootPath, String storageRegion, String index, String year){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size()-1, storageRegion, index, year);
    }

    public static List<BRFSPath> scanDirs(String rootPath, String storageRegion, String index, String year, String month){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size()-1, storageRegion, index, year, month);
    }

    public static List<BRFSPath> scanDirs(String rootPath, String storageRegion, String index, String year, String month, String day){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size()-1, storageRegion, index, year, month, day);
    }

    public static List<BRFSPath> scanFile(String rootPath){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size());
    }

    public static List<BRFSPath> scanFile(String rootPath, String storageRegion){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size(), storageRegion);
    }

    public static List<BRFSPath> scanFile(String rootPath, String storageRegion, String index){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size(), storageRegion, index);
    }

    public static List<BRFSPath> scanFile(String rootPath, String storageRegion, String index, String year){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size(), storageRegion, index, year);
    }

    public static List<BRFSPath> scanFile(String rootPath, String storageRegion, String index, String year, String month){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size(), storageRegion, index, year, month);
    }

    public static List<BRFSPath> scanFile(String rootPath, String storageRegion, String index, String year, String month, String day){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size(), storageRegion, index, year, month, day);
    }

    public static List<BRFSPath> scanFile(String rootPath, String storageRegion, String index, String year, String month, String day, String hour){
        return scanFiles(rootPath, BRFSPath.PATHLIST.size(), storageRegion, index, year, month, day, hour);
    }

    /**
     * 遍历BRFS文件存储目录
     * @param rootPath
     * @param end 遍历的深度
     * @param dirs 参数列表，不允许出现空值，从左到右分别为， storageRegion,index,year,month,day,hour
     * @return
     */
    public static List<BRFSPath> scanFiles(String rootPath, int end,  String... dirs){
        Map<String,String> map = new HashMap<>();
        int size = BRFSPath.PATHLIST.size();
        if(end <=0 || end>size){
            throw new IllegalArgumentException("param end invaild,the max value is"+size+", but end is " + end);
        }
        int length = 0;
        if(dirs != null && dirs.length > 0){
            length = dirs.length;
            if(length > BRFSPath.PATHLIST.size()){
                throw new IllegalArgumentException("can't scan deep "+length+", the max value is"+BRFSPath.PATHLIST.size());
            }
            String value = null;
            String key = null;
            for(int i = 0; i<length; i++){
                key = BRFSPath.PATHLIST.get(i);
                value = dirs[i];
                if(value == null || value.trim().isEmpty()){
                    throw new IllegalArgumentException("path can't have null!! "+key+" is null");
                }
                map.put(key,value);
            }

        }
        return listFiles(rootPath,map,BRFSPath.PATHLIST.subList(0,end),length);

    }

    /***
     * 根据指定路径深度遍历文件
     * @param rootPath
     * @param paths
     * @param pathlevel
     * @param index
     * @return
     */
    public static List<BRFSPath> listFiles(String rootPath, Map<String, String> paths, final List<String> pathlevel, int index){
        String path = createPath(rootPath, paths);
        File file = new File(path);
        if(!file.exists()) {
            return null;
        }
        int size = pathlevel.size();
        List<BRFSPath> files = new ArrayList<>();
        // 符合要求的添加到队列中
        if(index == size){
            String key = pathlevel.get(index -1);
            BRFSPath ele = null;
            if(key.equals(BRFSPath.FILE)){
                if(file.isFile()){
                    ele = BRFSPath.getInstance(paths);
                }
            }else if(file.isDirectory()){
                ele = BRFSPath.getInstance(paths);
            }
            if(ele != null){
                files.add(ele);
            }
            return files;
        }
        String key = pathlevel.get(index);
        // 当为目录时
        if(file.isDirectory()) {
            String[] dirs = file.list();
            if(dirs == null || dirs.length == 0) {
                return files;
            }
            List<BRFSPath> tmpList = null;
            Map<String, String> tmp = null;
            for(String dir : dirs) {
                tmp = new HashMap<>();
                tmp.putAll(paths);
                tmp.put(key,dir);
                tmpList = listFiles(rootPath,tmp,pathlevel,index +1);
                if(tmpList == null || tmpList.isEmpty()){
                    continue;
                }
                files.addAll(tmpList);
            }

        }
        return files;
    }

    /**
     * 获取在该时间段的目录，
     * @param root
     * @param map
     * @param startTime
     * @param endTime
     * @param pathlevel
     * @param index
     * @return
     */
    public static List<BRFSPath> scanDurationDirs(String root,  Map<String,String> map, final Map<String,String> startTime, final Map<String,String> endTime, final List<String> pathlevel, int index){
        String path = createPath(root, map);
        File file = new File(path);
        if(!file.exists()) {
            return null;
        }
        int size = pathlevel.size();
        List<BRFSPath> files = new ArrayList<>();
        // 符合要求的添加到队列中
        if(index == size){
           return null;
        }
        String key = pathlevel.get(index);
        // 当为目录时
        if(file.isDirectory()) {
            String[] dirs = file.list();
            if(dirs == null || dirs.length == 0) {
                return files;
            }
            List<BRFSPath> tmpList = null;
            Map<String, String> tmp = null;
            BRFSPath nPath = null;
            for(String dir : dirs) {
                tmp = new HashMap<>();
                tmp.putAll(map);
                tmp.put(key,dir);
                if(isNeed(key,dir,startTime,endTime)){
                    nPath = BRFSPath.getInstance(tmp);
                    files.add(nPath);
                    continue;
                }
                tmpList = listFiles(root,tmp,pathlevel,index +1);
                if(tmpList == null || tmpList.isEmpty()){
                    continue;
                }
                files.addAll(tmpList);
            }

        }
        return files;
    }
    public static boolean isNeed(String key, String value, Map<String,String> startMap, Map<String,String> endMap){
        if(value == null || value.trim().isEmpty()){
            return false;
        }
        String start = startMap.get(key);
        String end = endMap.get(key);
        if(start == null && end == null){
            return false;
        }
        if(start != null && start.equals(end)){
            return false;
        }
        if(start != null && start.compareTo(value) > 0 ){
            return false;
        }
        return end.compareTo(value) > 0;
    }
    //扫描非法文件及目录
    public static List<BRFSPath> scanBRFSFiles(String root,Map<String,String> map, int index, FileFilterInterface filter){
        String path = createPath(root, map);
        File file = new File(path);
        if(!file.exists()) {
            return null;
        }
       List<BRFSPath> files = new ArrayList<>();
        // 当为目录时
        if(file.isDirectory()) {
            // 添加本目录
            if(filter.isAdd(root, map, false)){
                files.add(BRFSPath.getInstance(map));
            }
            if(!filter.isDeep(index,map)){
                return files;
            }
            // 添加子目录
            String[] dirs = file.list();
            if(dirs == null || dirs.length == 0) {
                return files;
            }
            String key = filter.getKey(index);

            List<BRFSPath> tmpList = null;
            Map<String, String> tmp = null;
            for(String dir : dirs) {
                tmp = new HashMap<>(map);
                tmp.put(key,dir);
                tmpList = scanBRFSFiles(root,tmp,index +1,filter);
                if(tmpList == null || tmpList.isEmpty()){
                    continue;
                }
                files.addAll(tmpList);
            }
        }
        //文件
        if(file.isFile() && filter.isAdd(root,map,true)){
            files.add(BRFSPath.getInstance(map));
        }
        return files;
    }


    public static String createPath(String rootPath, Map<String, String> paths){
        StringBuilder pathBuilder = new StringBuilder();
        pathBuilder.append(rootPath);

        if(paths.containsKey(BRFSPath.STORAGEREGION)) {
            pathBuilder.append(BRFSPath.FILE_SEPARATOR).append(paths.get(BRFSPath.STORAGEREGION));
        } else {
            return pathBuilder.toString();
        }

        if(paths.containsKey(BRFSPath.INDEX)) {
            pathBuilder.append(BRFSPath.FILE_SEPARATOR).append(paths.get(BRFSPath.INDEX));
        } else {
            return pathBuilder.toString();
        }
        if(paths.containsKey(BRFSPath.YEAR)) {
            pathBuilder.append(BRFSPath.FILE_SEPARATOR).append(paths.get(BRFSPath.YEAR));
        } else {
            return pathBuilder.toString();
        }
        if(paths.containsKey(BRFSPath.MONTH)) {
            pathBuilder.append(BRFSPath.FILE_SEPARATOR).append(paths.get(BRFSPath.MONTH));
        } else {
            return pathBuilder.toString();
        }

        if(paths.containsKey(BRFSPath.DAY)) {
            pathBuilder.append(BRFSPath.FILE_SEPARATOR).append(paths.get(BRFSPath.DAY));
        } else {
            return pathBuilder.toString();
        }
        if(paths.containsKey(BRFSPath.TIME)) {
            pathBuilder.append(BRFSPath.FILE_SEPARATOR).append(paths.get(BRFSPath.TIME));
        }else{
            return pathBuilder.toString();
        }
        if(paths.containsKey(BRFSPath.FILE)){
            pathBuilder.append(BRFSPath.FILE_SEPARATOR).append(paths.get(BRFSPath.FILE));
        }
        return pathBuilder.toString();
    }
}
