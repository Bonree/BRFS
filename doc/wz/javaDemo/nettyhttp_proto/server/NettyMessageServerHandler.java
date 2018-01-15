package com.bonree.brfs.nettyhttp.server;

import com.bonree.brfs.common.proto.FileDataProtos.FileDataReqRes;
import com.bonree.brfs.common.proto.NettyMessageProto.NettyMessageReqRes;
import com.bonree.brfs.common.proto.StorageNameProtos.StorageNameReqRes;
import com.bonree.brfs.common.proto.StorageNameProtos.StorageNameRequest;
import com.bonree.brfs.common.proto.StorageNameProtos.StorageNameResponse;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

public class NettyMessageServerHandler extends ChannelInboundHandlerAdapter {
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		HttpContent httpRequest = (HttpContent) msg;
		ByteBuf bBuf = httpRequest.content();
		byte[] bytes = new byte[bBuf.readableBytes()];
		bBuf.readBytes(bytes);
		NettyMessageReqRes req = NettyMessageReqRes.parseFrom(bytes);
		NettyMessageReqRes.Builder builder = NettyMessageReqRes.newBuilder();
		builder.setSessionId(req.getSessionId()).setOptType(req.getOptType());

		if (NettyMessageReqRes.OptType.STORATE_NAME == req.getOptType()) {
			StorageNameRequest snRequest = req.getSnReqRes().getRequest();
			StorageNameResponse.Builder snBuilder = StorageNameResponse.newBuilder();
			if (StorageNameRequest.StorageNameOptType.CREATE == snRequest.getStorageNameOptType()) {
				System.out.println("create sn:" + snRequest);
				snBuilder.setCode(StorageNameResponse.ResultCode.SUCCESS);
				snBuilder.setDesc("create sn successfully!");

			} else if (StorageNameRequest.StorageNameOptType.UPDATE == snRequest.getStorageNameOptType()) {
				System.out.println("update sn:" + snRequest);
				snBuilder.setCode(StorageNameResponse.ResultCode.SUCCESS);
				snBuilder.setDesc("update sn successfully!");
			} else if (StorageNameRequest.StorageNameOptType.DELETE == snRequest.getStorageNameOptType()) {
				System.out.println("delete sn:" + snRequest);
				snBuilder.setCode(StorageNameResponse.ResultCode.SUCCESS);
				snBuilder.setDesc("delete sn successfully!");
			}
			StorageNameReqRes snResponse = StorageNameReqRes.newBuilder().setResponse(snBuilder.build()).build();
			builder.setSnReqRes(snResponse);
		} else if (NettyMessageReqRes.OptType.DATA_FILE == req.getOptType()) {
			FileDataReqRes dataRequest = req.getDataReqRes();
			if (FileDataReqRes.DataOptType.WRITE == dataRequest.getDataOptType()) {
				System.out.println("write data:");
				for (int i = 0; i < dataRequest.getDataCount(); i++) {
					System.out.println(dataRequest.getData(i));
				}
			} else if (FileDataReqRes.DataOptType.READ == dataRequest.getDataOptType()) {
				System.out.println("read data:");
				for (int i = 0; i < dataRequest.getFidCount(); i++) {
					System.out.println(dataRequest.getFid(i));
				}
			} else if (FileDataReqRes.DataOptType.DELETE == dataRequest.getDataOptType()) {
				System.out.println("删除方式:" + dataRequest.getDeleteDataType().name());
				System.out.println("删除时间段:" + dataRequest.getBeginTime() + "--" + dataRequest.getEndTime());
			}
		}
		NettyMessageReqRes responseContent = builder.build();
		ByteBuf byteBuf = Unpooled.wrappedBuffer(responseContent.toByteArray());
		FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
				HttpResponseStatus.OK, byteBuf);
		System.out.println("return result!");
		System.out.println(new String(responseContent.toByteArray()));
		ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
	}
	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		ctx.flush();
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.close();
	}
}
