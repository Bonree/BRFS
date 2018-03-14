package com.bonree.brfs.server.identification;

public interface Identification {

    public final static int SINGLE = 1;
    public final static int MULTI = 2;
    public final static int VIRTUAL = 3;

    public String getSingleIdentification();

    public String getMultiIndentification();

    public String getVirtureIdentification();

}
