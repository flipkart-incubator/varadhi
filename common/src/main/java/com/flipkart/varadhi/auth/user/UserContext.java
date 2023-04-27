package com.flipkart.varadhi.auth.user;

public interface UserContext {
    
    String getSubject();

    boolean isExpired();

    // TODO: enhance it to include more details like context around the authentication
}
