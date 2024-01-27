package com.flipkart.varadhi.db;


import lombok.Data;

@Data
class ZookeeperOptions {
    private String connectUrl = "127.0.0.1:2181";
    private int sessionTimeout = 60 * 1000;
    private int connectTimeout = 2000;
}
