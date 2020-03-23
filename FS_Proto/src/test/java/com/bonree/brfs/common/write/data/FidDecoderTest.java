package com.bonree.brfs.common.write.data;

import com.bonree.brfs.common.proto.FileDataProtos;
import org.junit.Test;

public class FidDecoderTest{
    @Test
    public void testDecoderFids(){
        String fid = "CAAQABgNIiAxY2EwOTE4N2JkNjE0YTJiYTU1M2VlZDc2ZGRiMTU2ZCit7Nqe5i0wgN3bAToDMjI3OgMyMjZAAEhX";
        System.out.println("fid length "+fid.length());
        int count = 1;
        FileDataProtos.Fid fids = null;
        long start = System.currentTimeMillis();
        for(int i = 0;i <count;i++){
            try{
                fids = FidDecoder.build(fid);
                System.out.println(fids);
            } catch(Exception e){
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("count : "+ count+ " ,time :"+(end -start) + "ms");
    }

}
