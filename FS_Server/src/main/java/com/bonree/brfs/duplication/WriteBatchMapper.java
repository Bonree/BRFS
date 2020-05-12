package com.bonree.brfs.duplication;

import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM;

import com.bonree.brfs.common.proto.DataTransferProtos.WriteBatch;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

@Provider
@Consumes(APPLICATION_OCTET_STREAM)
@Produces(APPLICATION_OCTET_STREAM)
public class WriteBatchMapper implements MessageBodyReader<WriteBatch>, MessageBodyWriter<WriteBatch> {

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return type == WriteBatch.class;
    }

    @Override
    public WriteBatch readFrom(Class<WriteBatch> type, Type genericType, Annotation[] annotations,
                                  MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
        throws IOException, WebApplicationException {
        return WriteBatch.parseFrom(entityStream);
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return type == WriteBatch.class;
    }

    @Override
    public void writeTo(WriteBatch t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
                        MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
        throws IOException, WebApplicationException {
        t.writeTo(entityStream);
    }

}
