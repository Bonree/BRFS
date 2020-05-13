package com.bonree.brfs.disknode;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.bonree.brfs.common.statistic.ReadStatCollector;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/stat")

public class StatResource {
    private ReadStatCollector readStatCollector;

    @Inject
    public StatResource(ReadStatCollector readStatCollector) {
        this.readStatCollector = readStatCollector;
    }

    @GET
    @Path("/read")
    @Produces(APPLICATION_JSON)
    public Map get() {
        return readStatCollector.popAll();
    }
}
