/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bonree.brfs.common.lifecycle;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.StringUtils;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scope;

/**
 * A scope that adds objects to the Lifecycle.  This is by definition also a lazy singleton scope.
 */
public class LifecycleScope implements Scope
{
  private static final Logger log = LoggerFactory.getLogger(LifecycleScope.class);
  private final Lifecycle.Stage stage;

  private Lifecycle lifecycle;
  private final List<Object> instances = new ArrayList<>();

  public LifecycleScope(Lifecycle.Stage stage)
  {
    this.stage = stage;
  }

  public void setLifecycle(Lifecycle lifecycle)
  {
    synchronized (instances) {
      this.lifecycle = lifecycle;
      for (Object instance : instances) {
        lifecycle.addAnnotatedInstance(instance, stage);
      }
    }
  }

  @Override
  public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped)
  {
    return new Provider<T>()
    {
      private volatile T value = null;

      @Override
      public synchronized T get()
      {
        if (value == null) {
          final T retVal = unscoped.get();

          synchronized (instances) {
            if (lifecycle == null) {
              instances.add(retVal);
            } else {
              try {
                lifecycle.addMaybeStartAnnotatedInstance(retVal, stage);
              }
              catch (Exception e) {
                log.warn(StringUtils.format("Caught exception when trying to create a[%s]", key), e);
                return null;
              }
            }
          }

          value = retVal;
        }

        return value;
      }
    };
  }
}
