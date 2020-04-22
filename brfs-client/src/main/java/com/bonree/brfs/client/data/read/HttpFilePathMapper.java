/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bonree.brfs.client.data.read;

import static com.bonree.brfs.client.utils.Strings.format;

import java.io.IOException;

import com.bonree.brfs.client.BRFSPath;
import com.bonree.brfs.client.discovery.Discovery.ServiceType;
import com.bonree.brfs.client.discovery.NodeSelector;
import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.client.utils.Retrys;
import com.bonree.brfs.client.utils.URIRetryable;
import com.bonree.brfs.client.utils.URIRetryable.TaskResult;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class HttpFilePathMapper implements FilePathMapper {
    private final OkHttpClient httpClient;
    private final NodeSelector nodeSelector;
    
    public HttpFilePathMapper(
            OkHttpClient httpClient,
            NodeSelector nodeSelector) {
        this.nodeSelector = nodeSelector;
        this.httpClient = httpClient;
    }

    @Override
    public String getFidByPath(String srName, BRFSPath path) {
        return Retrys.execute(new URIRetryable<String> (
                format("get fid of path[%s] in sr[%s]", path, srName),
                nodeSelector.getNodeHttpLocations(ServiceType.REGION),
                uri -> {
                    Request httpRequest = new Request.Builder()
                            .url(HttpUrl.get(uri)
                                    .newBuilder()
                                    .encodedPath("/catalog/fid")
                                    .addEncodedPathSegment(srName)
                                    .addQueryParameter("absPath", path.getPath())
                                    .build())
                            .get()
                            .build();
                    
                    try {
                        Response response = httpClient.newCall(httpRequest).execute();
                        if(response.code() == HttpStatus.CODE_OK) {
                            ResponseBody responseBody = response.body();
                            if(responseBody == null) {
                                return TaskResult.fail(new IllegalStateException("No response content is found"));
                            }
                            
                            return TaskResult.success(responseBody.string());
                        }
                        
                        return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                    } catch (IOException e) {
                        return TaskResult.retry(e);
                    }
                }));
    }

}
