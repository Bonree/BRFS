package com.bonree.brfs.duplication.configuration;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.bonree.brfs.common.jackson.Json;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("config")
public class ConfigurationResource {
    private static final Logger log = LoggerFactory.getLogger(ConfigurationResource.class);

    private final ServiceManager serviceManager;
    private final Service self;
    private final ObjectMapper jsonMapper;
    private final LocalStore localStore;

    @Inject
    public ConfigurationResource(ServiceManager serviceManager,
                                 Service self,
                                 @Json ObjectMapper jsonMapper,
                                 LocalStore localStore) {
        this.serviceManager = serviceManager;
        this.self = self;
        this.jsonMapper = jsonMapper;
        this.localStore = localStore;
    }

    @POST
    @Consumes(APPLICATION_JSON)
    public Response config(AccessConfig config) throws JsonProcessingException {
        String payload = jsonMapper.writeValueAsString(config);

        Service service = serviceManager.getServiceById(self.getServiceGroup(), self.getServiceId());
        if (service == null) {
            throw new IllegalStateException("can not find service for me");
        }

        if (!Objects.equals(service.getPayload(), payload)) {
            try {
                serviceManager.updateService(service.getServiceGroup(), service.getServiceId(), payload);
                localStore.store(payload);
            } catch (Exception e) {
                log.error("udpate service error", e);
                return Response.serverError().build();
            }
        }

        return Response.ok().build();
    }
}
