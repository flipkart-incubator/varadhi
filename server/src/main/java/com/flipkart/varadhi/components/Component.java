package com.flipkart.varadhi.components;

import com.flipkart.varadhi.cluster.ClusterManager;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public interface Component {
    Future<Void> start(Vertx vertx, ClusterManager clusterManager);
    Future<Void> shutdown(Vertx vertx, ClusterManager clusterManager);
}
