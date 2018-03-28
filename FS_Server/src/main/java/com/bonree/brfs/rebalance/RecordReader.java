package com.bonree.brfs.rebalance;

import java.io.IOException;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月28日 上午11:07:07
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: balance时对record进行记录
 ******************************************************************************/
public interface RecordReader<T> {

    T readerRecord() throws IOException;
}
