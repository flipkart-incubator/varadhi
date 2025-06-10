package com.flipkart.varadhi.common;

import lombok.Data;

@Data
public class ZookeeperConnectConfig {

    private String connectUrl = "127.0.0.1:2181";

    private int sessionTimeoutMs = 30000;

    private int connectTimeoutMs = 5000;

    private String namespace = "";
}
