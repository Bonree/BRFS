/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bonree.brfs.common.lifecycle;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.StringUtils;
import com.google.common.collect.Lists;

public class Lifecycle {
    private static final Logger log = LoggerFactory.getLogger(Lifecycle.class);

    public enum Stage {
        INIT,
        NORMAL,
        SERVER
    }
    
    private enum State {
      /** Lifecycle's state before {@link #start()} is called. */
      NOT_STARTED,
      /** Lifecycle's state since {@link #start()} and before {@link #stop()} is called. */
      RUNNING,
      /** Lifecycle's state since {@link #stop()} is called. */
      STOP
    }
    
    private final NavigableMap<Stage, CopyOnWriteArrayList<LifeCycleObject>> lives;
    private final Lock startStopLock = new ReentrantLock();
    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);
    private Stage currStage = null;
    private final AtomicBoolean shutdownHookRegistered = new AtomicBoolean(false);
    
    public Lifecycle() {
        this.lives = new TreeMap<>();
        for (Stage stage : Stage.values()) {
            lives.put(stage, new CopyOnWriteArrayList<>());
        }
    }
    
    public <T> T addAnnotatedInstance(T object) {
        addLifeCycleObject(new AnnotatedLifeCycleObject(object));
        return object;
    }
    
    public <T> T addAnnotatedInstance(T object, Stage stage) {
        addLifeCycleObject(new AnnotatedLifeCycleObject(object), stage);
        return object;
    }
    
    public Closeable addCloseable(Closeable closeable) {
        addLifeCycleObject(new CloseableLifeCycleObject(closeable));
        return closeable;
    }
    
    public Closeable addCloseable(Closeable closeable, Stage stage) {
        addLifeCycleObject(new CloseableLifeCycleObject(closeable), stage);
        return closeable;
    }
    
    public void addLifeCycleObject(LifeCycleObject obj) {
        addLifeCycleObject(obj, Stage.NORMAL);
    }
    
    public void addLifeCycleObject(LifeCycleObject obj, Stage stage) {
        if (!startStopLock.tryLock()) {
            throw new IllegalStateException("Cannot add a handler in the process of Lifecycle starting or stopping");
          }
          try {
            if (!state.get().equals(State.NOT_STARTED)) {
              throw new IllegalStateException("Cannot add a handler after the Lifecycle has started, it doesn't work that way.");
            }
            lives.get(stage).add(obj);
          }
          finally {
            startStopLock.unlock();
          }
    }
    
    public <T> T addMaybeStartAnnotatedInstance(T object) throws Exception {
        addMaybeStartObject(new AnnotatedLifeCycleObject(object));
        return object;
    }
    
    public <T> T addMaybeStartAnnotatedInstance(T object, Stage stage) throws Exception {
        addMaybeStartObject(new AnnotatedLifeCycleObject(object), stage);
        return object;
    }
    
    public Closeable addMaybeStartCloseable(Closeable closeable) throws Exception {
        addMaybeStartObject(new CloseableLifeCycleObject(closeable));
        return closeable;
    }
    
    public Closeable addMaybeStartCloseable(Closeable closeable, Stage stage) throws Exception {
        addMaybeStartObject(new CloseableLifeCycleObject(closeable), stage);
        return closeable;
    }
    
    public void addMaybeStartObject(LifeCycleObject obj) throws Exception {
        addMaybeStartObject(obj, Stage.NORMAL);
    }
    
    public void addMaybeStartObject(LifeCycleObject obj, Stage stage) throws Exception {
        if (!startStopLock.tryLock()) {
            if (state.get().equals(State.STOP)) {
                throw new IllegalStateException("Cannot add a handler in the process of Lifecycle stopping");
            }
            startStopLock.lock();
        }
        try {
            if (state.get().equals(State.STOP)) {
                throw new IllegalStateException("Cannot add a handler after the Lifecycle has stopped");
            }
            if (state.get().equals(State.RUNNING)) {
                if (stage.compareTo(currStage) <= 0) {
                    obj.start();
                }
            }
            lives.get(stage).add(obj);
        } finally {
            startStopLock.unlock();
        }
    }
    
    public void start() throws Exception {
        startStopLock.lock();
        try {
          if (!state.get().equals(State.NOT_STARTED)) {
            throw new IllegalStateException("Already started");
          }
          if (!state.compareAndSet(State.NOT_STARTED, State.RUNNING)) {
            throw new IllegalStateException("stop() is called concurrently with start()");
          }
          for (Map.Entry<Stage, ? extends List<LifeCycleObject>> e : lives.entrySet()) {
            currStage = e.getKey();
            log.info("Starting lifecycle stage [%s]", currStage.name());
            for (LifeCycleObject obj : e.getValue()) {
              obj.start();
            }
          }
          log.info("Successfully started lifecycle");
        }
        finally {
          startStopLock.unlock();
        }
    }
    
    public void stop() {
        if(!state.compareAndSet(State.RUNNING, State.STOP)) {
            log.info("life cycle is not running now, just skip it!");
            return;
        }
        
        startStopLock.lock();
        try {
            RuntimeException thrown = null;
            
            for(Stage stage : lives.navigableKeySet().descendingSet()) {
                log.info("Stopping lifecycle stage [%s]", stage);
                for(LifeCycleObject obj : Lists.reverse(lives.get(stage))) {
                    try {
                        obj.stop();
                    } catch (RuntimeException e) {
                        log.warn(StringUtils.format("Lifecycle encountered exception while stopping %s", obj), e);
                        if (thrown == null) {
                          thrown = e;
                        }
                    }
                }
            }
            
            if (thrown != null) {
                throw thrown;
            }
        } finally {
            startStopLock.unlock();
        }
    }
    
    public void join() throws InterruptedException {
        ensureShutdownHook();
        Thread.currentThread().join();
    }
    
    public void ensureShutdownHook()
    {
      if (shutdownHookRegistered.compareAndSet(false, true)) {
        Runtime.getRuntime().addShutdownHook(
            new Thread(
                new Runnable()
                {
                  @Override
                  public void run()
                  {
                    log.info("Lifecycle running shutdown hook");
                    stop();
                  }
                }
            )
        );
      }
    }
    
    public static interface LifeCycleObject {
        
        void start() throws Exception;
        
        void stop();
    }
    
    private static class AnnotatedLifeCycleObject implements LifeCycleObject {
        private final Object obj;
        
        public AnnotatedLifeCycleObject(Object obj) {
            this.obj = obj;
        }

        @Override
        public void start() throws Exception {
            log.info("start object[{}]", obj);
            
            for(Method m : obj.getClass().getMethods()) {
                if(m.isAnnotationPresent(LifecycleStart.class)) {
                    log.info("Invoking start method[%s] on object[%s].", m, obj);
                    m.invoke(obj);
                }
            }
        }

        @Override
        public void stop() {
            log.info("stop object[{}]", obj);
            
            for(Method m : obj.getClass().getMethods()) {
                if(m.isAnnotationPresent(LifecycleStop.class)) {
                    log.info("Invoking stop method[%s] on object[%s].", m, obj);
                    try {
                        m.invoke(obj);
                    } catch (Exception e) {
                        log.error(StringUtils.format("Exception occurred when closing object[{}]", obj), e);
                    }
                }
            }
        }
        
    }
    
    private static class CloseableLifeCycleObject implements LifeCycleObject {
        
        private final Closeable closeable;
        
        public CloseableLifeCycleObject(Closeable closeable) {
            this.closeable = closeable;
        }

        @Override
        public void start() throws Exception {
            // do nothing
        }

        @Override
        public void stop() {
            log.info("closing object [{}]", closeable);
            try {
                closeable.close();
            } catch (IOException e) {
                log.error(StringUtils.format("Exception occurred when closing object[{}]", closeable), e);
            }
        }
        
    }
}
