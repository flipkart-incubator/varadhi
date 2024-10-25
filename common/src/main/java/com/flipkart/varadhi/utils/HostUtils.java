package com.flipkart.varadhi.utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
public class HostUtils {
    @Getter
    private static String hostName;
    @Getter
    private static String hostAddress;

    public static void initHostUtils() throws UnknownHostException {
        if(hostName == null) {
            hostName = InetAddress.getLocalHost().getHostName();
        }
        if(hostAddress == null) {
            hostAddress = InetAddress.getLocalHost().getHostAddress();
        }
    }
}
