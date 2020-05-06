package com.bonree.brfs.resource.convertor;

import com.bonree.brfs.common.resource.vo.Load;

public class LoadConvertor {
    public Load convetoLoad(double[] uptimes) {
        Load load = new Load();
        load.setMin1Load(uptimes[0]);
        load.setMin5Load(uptimes[1]);
        load.setMin15Load(uptimes[2]);
        return load;
    }
}
