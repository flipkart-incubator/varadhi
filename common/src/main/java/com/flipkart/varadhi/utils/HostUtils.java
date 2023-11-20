package com.flipkart.varadhi.utils;

import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
public class HostUtils {

    public static String getHostName() throws UnknownHostException {
        // debug to see how much time it takes, in case DNS resolution is taking time.
        log.debug("getHostName: started");
        String host = InetAddress.getLocalHost().getHostName();
        log.debug("getHostName: completed");
        return host;
    }
}
