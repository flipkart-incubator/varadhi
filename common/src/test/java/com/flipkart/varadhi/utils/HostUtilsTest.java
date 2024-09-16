package com.flipkart.varadhi.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class HostUtilsTest {

    @Test
    public void TestGetHostName() throws UnknownHostException {
        // dummy test.. no validations here.
        String host = HostUtils.getHostNameOrAddress(true);
        Assertions.assertNotNull(host);
        Assertions.assertEquals(InetAddress.getLocalHost().getHostName(), host);
    }

    @Test
    public void TestGetAddress() throws UnknownHostException {
        // dummy test.. no validations here.
        String host = HostUtils.getHostNameOrAddress(false);
        Assertions.assertNotNull(host);
        Assertions.assertEquals(InetAddress.getLocalHost().getHostAddress(), host);
    }
}
