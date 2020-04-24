package com.bonree.brfs.common.net.http.client;

import java.net.URI;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

public class HttpClose extends HttpEntityEnclosingRequestBase {

    public final static String METHOD_NAME = "CLOSE";

    public HttpClose() {
        super();
    }

    public HttpClose(final URI uri) {
        super();
        setURI(uri);
    }

    /**
     * @throws IllegalArgumentException if the uri is invalid.
     */
    public HttpClose(final String uri) {
        super();
        setURI(URI.create(uri));
    }

    @Override
    public String getMethod() {
        return METHOD_NAME;
    }

}
