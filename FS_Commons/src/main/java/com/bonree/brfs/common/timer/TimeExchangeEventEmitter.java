package com.bonree.brfs.common.timer;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.utils.TimeUtils;
import com.google.common.base.Supplier;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

public class TimeExchangeEventEmitter implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(TimeExchangeEventEmitter.class);
	
	private ScheduledExecutorService timer;
	private Map<Duration, ScheduledFuture<?>> durationRunners = new HashMap<Duration, ScheduledFuture<?>>();
	private ExecutorService eventExecutor;
	
	private ListMultimap<Duration, TimeExchangeListener> listeners = Multimaps.newListMultimap(
			new HashMap<Duration, Collection<TimeExchangeListener>>(),
			new Supplier<List<TimeExchangeListener>>() {

				@Override
				public List<TimeExchangeListener> get() {
					return new CopyOnWriteArrayList<TimeExchangeListener>();
				}
			});
	
	public TimeExchangeEventEmitter(int threadNum) {
		this.timer = Executors.newSingleThreadScheduledExecutor();
		this.eventExecutor = Executors.newFixedThreadPool(threadNum, new PooledThreadFactory("time_event_runner"));
	}
	
	public long getStartTime(String durationExpression) {
		return getStartTime(Duration.parse(durationExpression));
	}
	
	public long getStartTime(Duration duration) {
		return TimeUtils.prevTimeStamp(System.currentTimeMillis(), duration.toMillis());
	}

	@Override
	public void close() {
		clearDurations();
		
		timer.shutdown();
		eventExecutor.shutdown();
	}
	
	private void clearDurations() {
		for(Duration duration : new ArrayList<Duration>(durationRunners.keySet())) {
			removeAllListeners(duration.toString());
		}
	}
	
	public void addListener(String durationExpression, TimeExchangeListener listener) {
		addListener(Duration.parse(durationExpression), listener);
	}
	
	public void addListener(Duration duration, TimeExchangeListener listener) {
		synchronized (listeners) {
			List<TimeExchangeListener> listenerList = listeners.get(duration);
			if(listenerList.isEmpty()) {
				long now = System.currentTimeMillis();
				ScheduledFuture<?> runner = timer.scheduleAtFixedRate(new DurationRunner(duration),
				TimeUtils.nextTimeStamp(now, duration.toMillis()) - now,
				duration.toMillis(), TimeUnit.MILLISECONDS);
				
				durationRunners.put(duration, runner);
			}
			
			listenerList.add(listener);
		}
	}
	
	public boolean removeListener(String durationExpression, TimeExchangeListener listener) {
		return removeListener(Duration.parse(durationExpression), listener);
	}
	
	public boolean removeListener(Duration duration, TimeExchangeListener listener) {
		synchronized (listeners) {
			List<TimeExchangeListener> listenerList = listeners.get(duration);
			if(!listenerList.remove(listener)) {
				return false;
			}
			
			if(listenerList.isEmpty()) {
				ScheduledFuture<?> runner = durationRunners.remove(duration);
				runner.cancel(false);
			}
		}
		
		return true;
	}
	
	public void removeAllListeners(String durationExpression) {
		removeAllListeners(Duration.parse(durationExpression));
	}
	
	public void removeAllListeners(Duration duration) {
		synchronized (listeners) {
			List<TimeExchangeListener> listenerList = listeners.get(duration);
			if(!listenerList.isEmpty()) {
				listenerList.clear();
				ScheduledFuture<?> runner = durationRunners.remove(duration);
				runner.cancel(false);
			}
		}
	}
	
	private class DurationRunner implements Runnable {
		private final Duration duration;
		private long currentStartTime;
		
		public DurationRunner(Duration duration) {
			this.duration = duration;
			this.currentStartTime = getStartTime(duration);
		}

		@Override
		public void run() {
			eventExecutor.submit(new Runnable() {
				
				@Override
				public void run() {
					while(getStartTime(duration) <= currentStartTime) {
						Thread.yield();
					}
					
					for(TimeExchangeListener listener : listeners.get(duration)) {
						try {
							listener.timeExchanged(getStartTime(duration), duration);
						} catch (Exception e) {
							LOG.error("call time exchange listener error", e);
						}
					}
				}
			});
		}
		
	}
}
