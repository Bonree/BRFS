package com.bonree.brfs.nettyhttp.client;

import java.util.UUID;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.bonree.brfs.common.proto.NettyMessageProto.NettyMessageReqRes;
import com.bonree.brfs.common.proto.StorageNameProtos.StorageNameReqRes;
import com.bonree.brfs.common.proto.StorageNameProtos.StorageNameRequest;

public class NettyMessageClient {

	public static void main(String[] args) throws Exception {
		DefaultHttpClient httpclient = new DefaultHttpClient();
//		HttpHost proxy = new HttpHost("127.0.0.1", 8080);
//		httpclient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
		HttpPost httppost = new HttpPost("http://127.0.0.1:8080");
		ByteArrayEntity entity = new ByteArrayEntity(createMessage().toByteArray());
		httppost.setEntity(entity);
		HttpResponse response = httpclient.execute(httppost);
		HttpEntity receiveEntity = response.getEntity();
		byte[] content = EntityUtils.toByteArray(receiveEntity);
		System.out.println("----------------------------------------");
		System.out.println(response.getStatusLine());
		NettyMessageReqRes responseMsg=NettyMessageReqRes.parseFrom(content);
		System.out.println(responseMsg);
		System.out.println("success");

	}

	private static NettyMessageReqRes createMessage() {
		NettyMessageReqRes.Builder builder = NettyMessageReqRes.newBuilder();
		builder.setSessionId(UUID.randomUUID().toString());
		builder.setOptType(NettyMessageReqRes.OptType.STORATE_NAME);
		StorageNameReqRes snRequest = StorageNameReqRes.newBuilder()
				.setRequest(StorageNameRequest.newBuilder().setName("sdk").setUser("weizheng")
						.setStorageNameOptType(StorageNameRequest.StorageNameOptType.CREATE).build())
				.build();
		NettyMessageReqRes request = builder.setSnReqRes(snRequest).build();
		return request;
	}
}
