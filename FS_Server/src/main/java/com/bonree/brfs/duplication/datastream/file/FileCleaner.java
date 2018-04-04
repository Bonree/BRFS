package com.bonree.brfs.duplication.datastream.file;

import java.util.ArrayList;
import java.util.List;

import com.bonree.brfs.common.utils.LifeCycle;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;

/**
 * 文件清理类，清理{@link FileLounge}中符合特定条件的文件节点
 * 
 * @author chen
 *
 */
public class FileCleaner implements LifeCycle {
	private FileLounge fileLounge;
	private DiskNodeConnectionPool connectionPool;
	
	private List<CleanFilter> filters = new ArrayList<CleanFilter>();
	
	public FileCleaner(FileLounge fileLounge, DiskNodeConnectionPool connectionPool) {
		this.fileLounge = fileLounge;
		this.connectionPool = connectionPool;
	}
	
	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	public void addFilter(CleanFilter filter) {
		filters.add(filter);
	}
	
	/**
	 * 文件清理的过滤接口
	 * 
	 * @author chen
	 *
	 */
	public static interface CleanFilter {
		/**
		 * 符合清理条件则返回true
		 * 
		 * @param fileLimiter
		 * @return
		 */
		boolean clean(FileLimiter fileLimiter);
	}
}
