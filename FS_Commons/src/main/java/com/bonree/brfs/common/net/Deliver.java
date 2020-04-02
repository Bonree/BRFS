package com.bonree.brfs.common.net;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date: 19-3-1下午3:52
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description:
 ******************************************************************************/
public interface Deliver extends Closeable {

    public boolean sendWriterMetric(Map<String, Object> data);

    public boolean sendReaderMetric(Map<String, Object> data);
    
    static Deliver NOOP = new Deliver() {
        
        @Override
        public void close() throws IOException {}
        
        @Override
        public boolean sendWriterMetric(Map<String, Object> data) {
            return true;
        }
        
        @Override
        public boolean sendReaderMetric(Map<String, Object> data) {
            return true;
        }
    };
}
