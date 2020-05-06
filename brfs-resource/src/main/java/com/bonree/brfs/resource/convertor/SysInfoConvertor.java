package com.bonree.brfs.resource.convertor;

import com.bonree.brfs.common.resource.vo.OSInfo;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.hyperic.sigar.OperatingSystem;

public class SysInfoConvertor {

    public OSInfo convertSysInfo(OperatingSystem sysInfo) {
        OSInfo os = new OSInfo();
        os.setHostName(getHostName());
        os.setOsArch(sysInfo.getArch());
        os.setOsCodeName(sysInfo.getVendorCodeName());
        os.setOsCpuEndian(sysInfo.getCpuEndian());
        os.setOsDataModel(sysInfo.getDataModel());
        os.setOsDescription(sysInfo.getDescription());
        os.setOsName(sysInfo.getName());
        os.setOsVendor(sysInfo.getVendor());
        os.setOsVendorVersion(sysInfo.getVendorVersion());
        os.setOsVersion(sysInfo.getVersion());
        return os;

    }

    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException var1) {
            return "unknown";
        }
    }

}
