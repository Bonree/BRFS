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

import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.write.data.FidDecoder;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class StringSubFidParser implements SubFidParser {

    @Override
    public List<Fid> readFids(InputStream content) throws Exception {
        ImmutableList.Builder<Fid> result = ImmutableList.builder();

        BufferedReader reader = new BufferedReader(new InputStreamReader(content));
        String fid = null;
        while ((fid = reader.readLine()) != null) {
            result.add(FidDecoder.build(fid));
        }

        return result.build();
    }

}
