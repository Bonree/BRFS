package com.bonree.brfs.disknode;

/**
 * {@WriteWorker}数组的封装类
 * 
 * 实现填充(fill)和遍历(forEach)数组元素的方法
 * 
 * @author chen
 *
 */
public class WriteWorkerGroup {
	private WriteWorker[] workers;
	
	public WriteWorkerGroup(int capacity) {
		this.workers = new WriteWorker[capacity];
	}
	
	//数组容量
	public int capacity() {
		return workers.length;
	}
	
	//返回原始数组对象
	public WriteWorker[] workerArray() {
		return workers;
	}
	
	//填充数组
	public void fill(Initializer initializer) {
		for(int i = 0; i < workers.length; i++) {
			workers[i] = initializer.init();
		}
	}
	
	//遍历数组元素
	public void forEach(Visitor visitor) {
		for(WriteWorker worker : workers) {
			try {
				visitor.visit(worker);
			} catch(Exception e) {}
		}
	}
	
	/**
	 * WriteWorker对象初始化接口
	 * 
	 * @author chen
	 *
	 */
	public static interface Initializer {
		WriteWorker init();
	}
	
	/**
	 * WriterWorker对象访问接口
	 * 
	 * @author chen
	 *
	 */
	public static interface Visitor {
		void visit(WriteWorker worker);
	}
}
