package com.bonree.brfs.duplication;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.bonree.brfs.common.statistic.ReadCountModel;
import com.bonree.brfs.common.statistic.WriteCountModel;
import com.bonree.brfs.common.statistic.WriteStatCollector;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/stat")

public class StatResource {
    private WriteStatCollector writeStatCollector;

    @Inject
    public StatResource(WriteStatCollector writeStatCollector) {
        this.writeStatCollector = writeStatCollector;
    }

    @GET
    @Path("/write")
    @Produces(APPLICATION_JSON)
    public Map get() {
        return writeStatCollector.popAll();
    }

    @GET
    @Path("/hello")
    @Produces(APPLICATION_JSON)
    public Map get1() {
        HashMap hashMap = new HashMap();
        hashMap.put("sr", new WriteCountModel(2));
        return hashMap;

    }

    @GET
    @Path("/hello1")
    @Produces(APPLICATION_JSON)
    public Map get2() {
        HashMap hashMap = new HashMap();
        hashMap.put("sr", new ReadCountModel(2, "2"));
        return hashMap;

    }

    @GET
    @Path("/hello3")
    @Produces(APPLICATION_JSON)
    public Map get3() {
        HashMap hashMap = new HashMap();
        hashMap.put("sr", new WriteCountModel(2));
        return hashMap;

    }
}
