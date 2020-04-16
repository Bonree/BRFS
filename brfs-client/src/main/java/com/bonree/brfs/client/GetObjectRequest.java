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
package com.bonree.brfs.client;

import static java.util.Objects.requireNonNull;

import java.nio.file.Path;

import com.bonree.brfs.client.utils.Range;
import com.bonree.brfs.client.utils.Strings;

public final class GetObjectRequest {
    private final String srName;
    private final String fid;
    private final Path filePath;
    private final Range range;
    
    private GetObjectRequest(String srName, String fid, Path filePath, Range range) {
        this.srName = requireNonNull(srName);
        this.fid = fid;
        this.filePath = filePath;
        this.range = range;
    }

    public String getStorageRegionName() {
        return srName;
    }
    
    public String getFID() {
        return fid;
    }
    
    public Path getPath() {
        return filePath;
    }
    
    public Range getRange() {
        return range;
    }
    
    static GetObjectRequest of(String srName, String fid) {
        return new GetObjectRequest(srName, fid, null, null);
    }
    
    static GetObjectRequest of(String srName, Path filePath) {
        return new GetObjectRequest(srName, null, filePath, null);
    }
    
    static GetObjectRequest of(String srName, String fid, long offset) {
        return of(srName, fid, offset, Long.MAX_VALUE);
    }
    
    static GetObjectRequest of(String srName, String fid, long offset, long size) {
        if(offset < 0 || size < 0) {
            throw new IllegalArgumentException(
                    Strings.format("offset and size should be greater or equal to 0, but [%d, %d]",
                            offset,
                            size));
        }
        
        return new GetObjectRequest(srName, fid, null, new Range(offset, size));
    }
    
    static GetObjectRequest of(String srName, Path filePath, long offset) {
        return of(srName, filePath, offset, Long.MAX_VALUE);
    }
    
    static GetObjectRequest of(String srName, Path filePath, long offset, long size) {
        if(offset < 0 || size < 0) {
            throw new IllegalArgumentException(
                    Strings.format("offset and size should be greater or equal to 0, but [%d, %d]",
                            offset,
                            size));
        }
        
        return new GetObjectRequest(srName, null, filePath, new Range(offset, size));
    }
}
