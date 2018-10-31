package com.bonree.brfs.common.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BRFSPath{
    public final static String FILE_SEPARATOR = "/";
    public final static String STORAGEREGION = "SOTRAGE_REGION";
    public final static String INDEX = "INDEX";
    public final static String YEAR = "YEAR";
    public final static String MONTH = "MONTH";
    public final static String DAY = "DAY";
    public final static String TIME = "TIME";
    public final static String FILE = "FILE";
    public final static List<String> PATHLIST = Arrays.asList(new String[] { STORAGEREGION, INDEX, YEAR, MONTH, DAY, TIME, FILE });
    private String storageRegion=null;
    private String index = null;
    private String year = null;
    private String month = null;
    private String day = null;
    private String hourMinSecond = null;
    private String fileName = null;

    public String getStorageRegion(){
        return storageRegion;
    }

    public void setStorageRegion(String storageRegion){
        this.storageRegion = storageRegion;
    }

    public String getIndex(){
        return index;
    }

    public void setIndex(String index){
        this.index = index;
    }

    public String getYear(){
        return year;
    }

    public void setYear(String year){
        this.year = year;
    }

    public String getMonth(){
        return month;
    }

    public void setMonth(String month){
        this.month = month;
    }

    public String getDay(){
        return day;
    }

    public void setDay(String day){
        this.day = day;
    }

    public String getHourMinSecond(){
        return hourMinSecond;
    }

    public void setHourMinSecond(String hourMinSecond){
        this.hourMinSecond = hourMinSecond;
    }

    public String getFileName(){
        return fileName;
    }

    public void setFileName(String fileName){
        this.fileName = fileName;
    }

    public static BRFSPath getInstance(String storageRegion, String index, String year, String month, String day, String hourMinSecond, String fileName){
        BRFSPath ele = new BRFSPath();
        ele.setStorageRegion(storageRegion);
        ele.setIndex(index);
        ele.setYear(year);
        ele.setMonth(month);
        ele.setDay(day);
        ele.setHourMinSecond(hourMinSecond);
        ele.setFileName(fileName);
        return ele;
    }
    public String toString(){
        StringBuilder path = new StringBuilder();
        if(storageRegion != null){
            path.append(storageRegion);
        }else{
            return path.toString();
        }
        if(index != null){
            path.append("/").append(index);
        }else{
            return path.toString();
        }
        if(year != null){
            path.append("/").append(year);
        }else{
            return path.toString();
        }
        if(month != null){
            path.append("/").append(month);
        }else{
            return path.toString();
        }
        if(day != null){
            path.append("/").append(day);
        }else{
            return path.toString();
        }
        if(hourMinSecond != null){
            path.append("/").append(hourMinSecond);
        }else{
            return path.toString();
        }
        if(fileName != null){
            path.append("/").append(fileName);
        }
        return path.toString();
    }

    public static BRFSPath getInstance(Map<String,String> content){
        if(content == null || content.isEmpty()){
            return null;
        }
        BRFSPath obj = new BRFSPath();
        if(content.containsKey(STORAGEREGION)){
            obj.setStorageRegion(content.get(STORAGEREGION));
        }else{
            return obj;
        }

        if(content.containsKey(INDEX)){
            obj.setIndex(content.get(INDEX));
        }else{
            return obj;
        }

        if(content.containsKey(YEAR)){
            obj.setYear(content.get(YEAR));
        }else{
            return obj;
        }

        if(content.containsKey(MONTH)){
            obj.setMonth(content.get(MONTH));
        }else{
            return obj;
        }

        if(content.containsKey(DAY)){
            obj.setDay(content.get(DAY));
        }else{
            return obj;
        }

        if(content.containsKey(TIME)){
            obj.setHourMinSecond(content.get(TIME));
        }else{
            return obj;
        }
        if(content.containsKey(FILE)){
            obj.setFileName(content.get(FILE));
        }
        return obj;
    }
}
