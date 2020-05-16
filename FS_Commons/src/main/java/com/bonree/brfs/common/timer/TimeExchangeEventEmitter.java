package com.bonree.brfs.common.timer;

import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.utils.TimeUtils;
import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ManageLifecycle
public class TimeExchangeEventEmitter implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(TimeExchangeEventEmitter.class);

    private final ScheduledExecutorService timer;
    private final Map<Duration, ScheduledFuture<?>> durationRunners = new HashMap<>();
    private final ExecutorService eventExecutor;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ConcurrentMap<Duration, List<TimeExchangeListener>> listeners = new ConcurrentHashMap<>();

    @Inject
    public TimeExchangeEventEmitter() {
        this.timer = Executors.newSingleThreadScheduledExecutor();
        this.eventExecutor = Executors.newFixedThreadPool(2, new PooledThreadFactory("time_event_runner"));
    }

    public long getStartTime(Duration duration) {
        return TimeUtils.prevTimeStamp(System.currentTimeMillis(), duration.toMillis());
    }

    @Override
    @LifecycleStop
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            log.warn("time event emitter has been closed, no need to close it again!");
            return;
        }

        for (Duration duration : listeners.keySet()) {
            ScheduledFuture<?> runner = durationRunners.remove(duration);
            runner.cancel(false);
        }

        timer.shutdown();
        eventExecutor.shutdown();
    }

    public void addListener(String durationExpression, TimeExchangeListener listener) {
        addListener(Duration.parse(durationExpression), listener);
    }

    public void addListener(Duration duration, TimeExchangeListener listener) {
        if (closed.get()) {
            throw new IllegalStateException("time event emitter is closed, can not add listener");
        }

        listeners.compute(duration, (d, list) -> {
            if (list == null) {
                list = new CopyOnWriteArrayList<>();
                long now = System.currentTimeMillis();
                ScheduledFuture<?> runner = timer.scheduleAtFixedRate(
                    new DurationRunner(duration),
                    TimeUtils.nextTimeStamp(now, duration.toMillis()) - now,
                    duration.toMillis(),
                    TimeUnit.MILLISECONDS);

                durationRunners.put(duration, runner);
            }

            list.add(listener);
            return list;
        });
    }

    public boolean removeListener(String durationExpression, TimeExchangeListener listener) {
        return removeListener(Duration.parse(durationExpression), listener);
    }

    public boolean removeListener(Duration duration, TimeExchangeListener listener) {
        AtomicBoolean result = new AtomicBoolean(false);
        listeners.compute(duration, (d, list) -> {
            if (list == null) {
                return null;
            }

            result.set(list.remove(listener));
            if (list.isEmpty()) {
                ScheduledFuture<?> runner = durationRunners.remove(d);
                runner.cancel(false);
                return null;
            }

            return list;
        });

        return result.get();
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
                    long startTime = getStartTime(duration);
                    while (startTime <= currentStartTime) {
                        Thread.yield();
                        startTime = getStartTime(duration);
                    }

                    currentStartTime = startTime;
                    for (TimeExchangeListener listener : listeners.get(duration)) {
                        try {
                            listener.timeExchanged(currentStartTime, duration);
                        } catch (Exception e) {
                            log.error("call time exchange listener error", e);
                        }
                    }
                }
            });
        }

    }
}
