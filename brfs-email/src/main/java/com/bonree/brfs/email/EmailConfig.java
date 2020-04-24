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

package com.bonree.brfs.email;

import com.bonree.brfs.common.utils.StringUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Calendar;

public class EmailConfig {
    @JsonProperty("smtp")
    private String smtp = "mail.bonree.com";

    @JsonProperty("smtp.port")
    private int smtpPort = 25;

    @JsonProperty("send.user")
    private String sendUser = "redalert@bonree.com";

    @JsonProperty("send.user.password")
    private String sendPasswd = "alert!^*90";

    @JsonProperty("use_ssl")
    private boolean useSSL = false;

    @JsonProperty("recipient")
    private String recipient = "zhucg@bonree.com, chenyp@bonree.com";

    @JsonProperty("header")
    private String header = "BRFS 文件集群";

    @JsonProperty("model")
    private String model = "BRFS 服务";

    @JsonProperty("company")
    private String company = "北京博睿宏远数据科技股份有限公司 版权所有   京 ICP备 08104257 号 京公网安备 1101051190";

    @JsonProperty("copyright")
    private String copyright = StringUtils.format("Copyright ©2007-%i All rights reserved.",
                                                  Calendar.getInstance().get(Calendar.YEAR));

    @JsonProperty("pool.size")
    private int poolSize = 3;

    @JsonProperty("switch")
    private boolean swith = false;

    public String getSmtp() {
        return smtp;
    }

    public void setSmtp(String smtp) {
        this.smtp = smtp;
    }

    public int getSmtpPort() {
        return smtpPort;
    }

    public void setSmtpPort(int smtpPort) {
        this.smtpPort = smtpPort;
    }

    public String getSendUser() {
        return sendUser;
    }

    public void setSendUser(String sendUser) {
        this.sendUser = sendUser;
    }

    public String getSendPasswd() {
        return sendPasswd;
    }

    public void setSendPasswd(String sendPasswd) {
        this.sendPasswd = sendPasswd;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    public String getRecipient() {
        return recipient;
    }

    public void setRecipient(String recipient) {
        this.recipient = recipient;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getCopyright() {
        return copyright;
    }

    public void setCopyright(String copyright) {
        this.copyright = copyright;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public boolean isSwith() {
        return swith;
    }

    public void setSwith(boolean swith) {
        this.swith = swith;
    }

}
