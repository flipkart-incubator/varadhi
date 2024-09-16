package com.flipkart.varadhi.utils;

import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
public class HostUtils {

    public static String getHostNameOrAddress(boolean requireHostName) throws UnknownHostException {
        if(requireHostName) {
            // debug to see how much time it takes, in case DNS resolution is taking time.
            log.debug("getHostName: started");
            String host = InetAddress.getLocalHost().getHostName();
            log.debug("getHostName: completed");
            return host;
        } else {
            log.debug("getAddress: started");
            String address = InetAddress.getLocalHost().getHostAddress();
            log.debug("getAddress: completed");
            return address;
        }
    }
}
