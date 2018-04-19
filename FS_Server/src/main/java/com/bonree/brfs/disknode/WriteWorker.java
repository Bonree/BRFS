package com.bonree.brfs.disknode;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.ThreadPoolUtil;
import com.bonree.brfs.disknode.server.handler.WriteData;

/**
 * 负责写数据到文件的Worker类，一个worker对应一个线程。
 * 
 * @author chen
 *
 */
public class WriteWorker implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(WriteWorker.class);
	
	private static final int DEFAULT_QUEUE_CAPACITY = 100;
	private ArrayBlockingQueue<InputEvent> itemQueue = new ArrayBlockingQueue<InputEvent>(DEFAULT_QUEUE_CAPACITY);
	
	private volatile boolean isQuit = false;
	
	public void put(InputEvent item) {
		try {
			itemQueue.put(item);
		} catch (InterruptedException e) {
			item.getInputEventCallback().error(e);
		}
	}
	
	public void quit() {
		isQuit = true;
	}

	@Override
	public void run() {
		while(!isQuit) {
			try {
				InputEvent inputEvent = itemQueue.take();
				
				LOG.debug("TAKE INPUT EVENT[{}]", inputEvent.getWriter().getFilePath());
				DiskWriter writer = inputEvent.getWriter();
				
				try {
					WriteData item = inputEvent.getItem();
					writer.beginWriting(item.getSequence());
					writer.write(item.getBytes());
					
					InputResult inputResult = writer.endWriting();
					ThreadPoolUtil.commonPool().execute(() -> inputEvent.getInputEventCallback().complete(inputResult));
				} catch (IOException e) {
					LOG.error("back writing", e);
					writer.backWriting();
					ThreadPoolUtil.commonPool().execute(() -> inputEvent.getInputEventCallback().error(e));
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
