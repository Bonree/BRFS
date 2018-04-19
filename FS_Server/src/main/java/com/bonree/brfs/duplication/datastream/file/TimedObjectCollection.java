package com.bonree.brfs.duplication.datastream.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * 
 * 按时间段创建对象，不同的时间段获取不同的对象。
 * 
 * @ThreadSafe
 * 
 * @author chen
 *
 * @param <T>
 */
public class TimedObjectCollection<T> {
	private final long intervalMillis;

	private ObjectBuilder<T> objectBuilder;
	private Map<Long, T> objects = new HashMap<Long, T>(8);

	public TimedObjectCollection(long interval, TimeUnit unit,
			ObjectBuilder<T> builder) {
		this.intervalMillis = unit.toMillis(interval);
		this.objectBuilder = builder;
	}

	public long getTimeInterval(long time) {
		return time / intervalMillis;
	}

	public List<TimedObject<T>> allObjects() {
		ArrayList<TimedObject<T>> list = new ArrayList<TimedObject<T>>();
		synchronized (objects) {
			for(Entry<Long, T> entry : objects.entrySet()) {
				list.add(new TimedObject<T>(entry.getKey(), entry.getValue()));
			}
		}
		
		return list;
	}

	public T get() {
		return get(System.currentTimeMillis());
	}

	public T get(long timestamp) {
		long timeInterval = getTimeInterval(timestamp);
		T obj = objects.get(timeInterval);
		if (obj == null) {
			synchronized (objects) {
				obj = objects.get(timeInterval);
				if (obj == null) {
					obj = objectBuilder.build();
					objects.put(timeInterval, obj);
				}
			}
		}

		return obj;
	}
	
	/**
	 * 删除某个时间区间的对象
	 * @param timeInterval
	 */
	public void remove(long timeInterval) {
		synchronized (objects) {
			objects.remove(timeInterval);
		}
	}

	/**
	 * 
	 * 新对象的创建接口
	 * 
	 * @author chen
	 *
	 * @param <T>
	 */
	public static interface ObjectBuilder<T> {
		T build();
	}

	public static class TimedObject<T> {
		private final long timeInterval;
		private final T object;

		private TimedObject(long timeInterval, T object) {
			this.timeInterval = timeInterval;
			this.object = object;
		}
		
		public long getTimeInterval() {
			return timeInterval;
		}

		public T getObj() {
			return object;
		}

	}
}
