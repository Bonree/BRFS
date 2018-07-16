package com.bonree.brfs.duplication;

import java.util.UUID;

import com.bonree.brfs.common.data.utils.Base64;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.write.data.FidDecoder;
import com.bonree.brfs.common.write.data.FidEncoder;

public class Test {

	public static void main(String[] args) throws Exception {
		Fid.Builder b = Fid.newBuilder();
		b.setVersion(0);
		b.setCompress(0);
		b.setStorageNameCode(100);
		b.setUuid(UUID.randomUUID().toString().replaceAll("-", ""));
		b.setTime(System.currentTimeMillis());
		b.addServerId(123).addServerId(124);
		b.setOffset(1234);
		b.setSize(33455);
		b.setDuration("P1H");
		
		byte[] bs = b.build().toByteArray();
		
		System.out.println("length : " + bs.length);
		String s1 = Base64.encodeToString(bs, Base64.DEFAULT);
		System.out.println("s1 : " + s1.length());
		
		String s2 = FidEncoder.build(b.build());
		System.out.println("s2 : " + s2.length());
		System.out.println("[" + s2 + "]");
		
		Fid fid = FidDecoder.build(s2);
		System.out.println(fid);
	}

}
