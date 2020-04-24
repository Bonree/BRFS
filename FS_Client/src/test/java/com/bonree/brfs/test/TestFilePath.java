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

package com.bonree.brfs.test;

import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.common.write.data.FidDecoder;
import java.io.File;
import java.util.concurrent.ExecutionException;

public class TestFilePath {

    /**
     * @param args
     *
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String storageName = "T_WINSDK_STAT_ERROR_SNAPSHOT";
        String fid = "CAAQABgNIiAxY2EwOTE4N2JkNjE0YTJiYTU1M2VlZDc2ZGRiMTU2ZCit7Nqe5i0wgN3bAToDMjI3OgMyMjZAAEhX";
        System.out.println(fid.length());
        Fid fidObj = FidDecoder.build(fid);

        StringBuilder nameBuilder = new StringBuilder(fidObj.getUuid());
        String[] serverList = new String[fidObj.getServerIdCount()];
        for (int i = 0; i < fidObj.getServerIdCount(); i++) {
            String id = fidObj.getServerId(i);
            nameBuilder.append('_').append(id);
            serverList[i] = id;
        }

        ReadObject readObject = new ReadObject();
        readObject.setSn(storageName);
        readObject.setIndex(0);
        readObject.setTime(fidObj.getTime());
        readObject.setDuration(fidObj.getDuration());
        readObject.setFileName(nameBuilder.toString());

        // readObject.setFilePath(FilePathBuilder.buildPath(fidObj,
        // timeCache.get(new
        // TimePair(TimeUtils.prevTimeStamp(fidObj.getTime(),
        // fidObj.getDuration()), fidObj.getDuration())),
        // storageName, serviceMetaInfo.getReplicatPot()));
        readObject.setOffset(fidObj.getOffset());
        readObject.setLength((int) fidObj.getSize());

        System.out.println(fidObj.getOffset());
        System.out.println(fidObj.getSize());

        System.out.println(buildPath(readObject));
    }

    private static String buildPath(ReadObject readObject) throws ExecutionException {
        StringBuilder pathBuilder = new StringBuilder();
        pathBuilder.append(File.separatorChar)
                   .append(readObject.getSn())
                   .append(File.separatorChar)
                   .append(readObject.getIndex())
                   .append(File.separatorChar)
                   .append(TimeUtils.timeInterval(TimeUtils.prevTimeStamp(readObject.getTime(), readObject.getDuration()),
                                                  readObject.getDuration()))
                   .append(File.separatorChar).append(readObject.getFileName());

        return pathBuilder.toString();
    }
}
