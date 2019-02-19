package com.bonree.brfs.common.write.data;

import com.bonree.brfs.common.proto.FileDataProtos;
import org.junit.Test;

public class FidDecoderTest{
    @Test
    public void testDecoderFids(){
        String fid = "CAAQABgAIiA0ZGY4MTgwOTMxYTY0ZjNlYTVmYzQwMjEzY2NjZDdkMijqwd37+ywwwM8kOgIyMjoCMjBAqIFLSIeQAw==";
        System.out.println("fid length "+fid.length());
        int count = 1000000;
        FileDataProtos.Fid fids = null;
        long start = System.currentTimeMillis();
        for(int i = 0;i <count;i++){
            try{
                fids = FidDecoder.build(fid);
            } catch(Exception e){
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("count : "+ count+ " ,time :"+(end -start) + "ms");
    }

}
