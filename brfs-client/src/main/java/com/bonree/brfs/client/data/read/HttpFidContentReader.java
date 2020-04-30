package com.bonree.brfs.client.data.read;

import static com.bonree.brfs.client.utils.Strings.format;

import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.common.proto.FileDataProtos;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.write.data.FileDecoder;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class HttpFidContentReader implements FidContentReader {

    public static final MediaType TEXT = MediaType.get("text/plain; charset=utf-8");

    private final OkHttpClient httpClient;

    public HttpFidContentReader(OkHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public InputStream read(URI service, String srName, Fid fidObj, long offset, long size, int uriIndex)
        throws Exception {
        RequestBody body = RequestBody.create(TEXT, toReadString(srName, fidObj, uriIndex));

        Request httpRequest = new Request.Builder()
            .url(HttpUrl.get(service)
                     .newBuilder()
                     .encodedPath("/read")
                     .build())
            .post(body)
            .build();

        Response response = httpClient.newCall(httpRequest).execute();
        if (response.code() == HttpStatus.CODE_OK) {
            ResponseBody responseBody = response.body();
            if (responseBody == null) {
                throw new IllegalStateException("No content is returned");
            }

            FileDataProtos.FileContent content = FileDecoder.contents(responseBody.bytes());
            return new ByteArrayInputStream(content.getData().toByteArray());
        }

        throw new IllegalStateException(format("Server error[%d]", response.code()));
    }

    private static byte[] toReadString(String srName, Fid fidObj, int index) {
        StringBuilder nameBuilder = new StringBuilder(fidObj.getUuid());
        for (int i = 0; i < fidObj.getServerIdCount(); i++) {
            String id = fidObj.getServerId(i);
            nameBuilder.append('_').append(id);
        }

        return Joiner.on(';')
            .useForNull("-")
            .join(srName,
                  index,
                  fidObj.getTime(),
                  fidObj.getDuration(),
                  nameBuilder.toString(),
                  null,
                  fidObj.getOffset(),
                  fidObj.getSize(),
                  0,
                  0,
                  "\n")
            .getBytes(Charsets.UTF_8);
    }
}
