package com.flipkart.varadhi.web.authn;

import com.flipkart.varadhi.entities.auth.UserContext;
import io.vertx.ext.auth.User;

public class VertxUserContext implements UserContext {

    private final User user;
    private final String subject;

    public VertxUserContext(User user) {
        this.user = user;
        this.subject = user.subject();
    }

    @Override
    public String getSubject() {
        return subject;
    }

    @Override
    public boolean isExpired() {
        return user.expired();
    }
}
