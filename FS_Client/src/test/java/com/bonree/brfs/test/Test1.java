package com.bonree.brfs.test;

import com.bonree.brfs.client.utils.FilePathBuilder;
import com.bonree.brfs.common.data.utils.Base64;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.common.write.data.FidDecoder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;


public class Test1 {
    
    private static LoadingCache<TimePair, String> timeCache = CacheBuilder.newBuilder()
    		.maximumSize(1024)
    		.build(new CacheLoader<TimePair, String>() {

				@Override
				public String load(TimePair pair) throws Exception {
					StringBuilder builder = new StringBuilder();
					builder.append(TimeUtils.formatTimeStamp(pair.time()))
					.append('_')
					.append(TimeUtils.formatTimeStamp(pair.time() + pair.duration()));
					return builder.toString();
				}
			});

	public static void main(String[] args) throws Exception {
		String fid = "CAAQABgAIiBiM2RkMGYxMmJmMWQ0ZjU1YWVmYWUyYWNkNjMwNzJiNyim57334SwwwM8kOgIyMDoCMjFAAEgd";
		
        long start = System.currentTimeMillis();
        for(int i = 0 ; i < 100000; i++) {
            Fid fidObj = FidDecoder.build(fid);
            //testpart base64:
            Base64.decode(fid, Base64.DEFAULT);
//            ReadObject readObject = new ReadObject();
            String str = FilePathBuilder.buildPath(fidObj,
    			timeCache.get(new TimePair(TimeUtils.prevTimeStamp(fidObj.getTime(), fidObj.getDuration()), fidObj.getDuration())),
    			"sn_br", 1);
//        	readObject.setFilePath(FilePathBuilder.buildPath(fidObj,
//        			timeCache.get(new TimePair(TimeUtils.prevTimeStamp(fidObj.getTime(), fidObj.getDuration()), fidObj.getDuration())),
//        			"sn_br", 1));
//        	readObject.setOffset(fidObj.getOffset());
//        	readObject.setLength((int) fidObj.getSize());
        }
        System.out.println("take : " + (System.currentTimeMillis() - start));
	}
	
	private static class TimePair {
    	private final long time;
    	private final long duration;
    	
    	public TimePair(long time, long duration) {
    		this.time = time;
    		this.duration = duration;
    	}
    	
    	public long time() {
    		return this.time;
    	}
    	
    	public long duration() {
    		return this.duration;
    	}

		@Override
		public int hashCode() {
			return (int) (this.time * 37 + this.duration);
		}

		@Override
		public boolean equals(Object obj) {
			if(obj == null) {
				return false;
			}
			
			if(!(obj instanceof TimePair)) {
				return false;
			}
			
			TimePair oth = (TimePair) obj; 
			
			return this.time == oth.time && this.duration == oth.duration;
		}
    }
}
