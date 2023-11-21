package com.flipkart.varadhi.entities;

public interface UserContext {

    String getSubject();

    boolean isExpired();

    // TODO: enhance it to include more details like context around the authentication
}
