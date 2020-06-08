package com.bonree.brfs.gui.server.stats;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.google.inject.Inject;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("stat")
public class StatResource {
    private StatReportor statReportor;
    private StatConfigs statConfigs;

    @Inject
    public StatResource(
        StatConfigs statConfigs,
        StatReportor statReportor) {
        this.statConfigs = statConfigs;
        this.statReportor = statReportor;
    }

    @GET
    @Path("{srName}")
    @Produces(APPLICATION_JSON)
    public List<BusinessStats> read(
        @PathParam("srName") String srName,
        @QueryParam("minutes") int minutes) {
        //return statReportor.getCount(srName, minutes);
        return null;
    }

    @GET
    @Path("/")
    @Produces(APPLICATION_JSON)
    public List<BusinessStats> getAll(@QueryParam("minutes") int minutes) {
        //return statReportor.getCount("", minutes);
        return null;
    }

}
