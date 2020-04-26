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

package com.bonree.brfs.client.data.read.connection;

import com.google.common.io.Closeables;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.function.Function;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PoolUtils;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.EvictionConfig;
import org.apache.commons.pool2.impl.EvictionPolicy;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class DataConnectionPool implements Closeable {
    private final KeyedObjectPool<URI, DataConnection> connections;

    public DataConnectionPool() {
        this.connections = PoolUtils.synchronizedPool(buildPool());

    }

    private static KeyedObjectPool<URI, DataConnection> buildPool() {
        GenericKeyedObjectPool<URI, DataConnection> pool = new GenericKeyedObjectPool<>(new UriConnectionFactory());
        pool.setTimeBetweenEvictionRunsMillis(30 * 1000);
        pool.setMinEvictableIdleTimeMillis(60 * 1000);
        pool.setBlockWhenExhausted(true);
        pool.setEvictionPolicy(new EvictionPolicy<DataConnection>() {

            @Override
            public boolean evict(EvictionConfig config, PooledObject<DataConnection> underTest, int idleCount) {
                return (System.currentTimeMillis() - underTest.getLastReturnTime()) > config.getIdleEvictTime();
            }
        });

        return pool;
    }

    public <T> DataRequestCall<T> newRequest(URI uri, Function<DataConnection, T> call) throws Exception {
        DataConnection connection = null;
        try {
            connection = connections.borrowObject(uri);

            final DataConnection param = connection;
            return () -> call.apply(param);
        } catch (Exception e) {
            if (connection != null) {
                connections.invalidateObject(uri, connection);
                connection = null;
            }

            throw e;
        } finally {
            // make sure the object is returned to the pool
            if (null != connection) {
                connections.returnObject(uri, connection);
            }
        }
    }

    private static class UriConnectionFactory implements KeyedPooledObjectFactory<URI, DataConnection> {

        @Override
        public PooledObject<DataConnection> makeObject(URI uri) throws Exception {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(uri.getHost(), uri.getPort()));

            return new DefaultPooledObject<>(new DataConnection(uri, socket));
        }

        @Override
        public void destroyObject(URI key, PooledObject<DataConnection> p) throws Exception {
            DataConnection connection = p.getObject();
            if (connection != null) {
                connection.close();
            }
        }

        @Override
        public boolean validateObject(URI key, PooledObject<DataConnection> p) {
            return true;
        }

        @Override
        public void activateObject(URI key, PooledObject<DataConnection> p) throws Exception {
        }

        @Override
        public void passivateObject(URI key, PooledObject<DataConnection> p) throws Exception {
        }

    }

    @Override
    public void close() throws IOException {
        Closeables.close(connections, false);
    }
}
