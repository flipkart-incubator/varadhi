package com.flipkart.varadhi.web;

import com.flipkart.varadhi.auth.AuthorizationOptions;
import com.flipkart.varadhi.auth.AuthorizationProvider;
import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.auth.ResourceAction;
import com.flipkart.varadhi.entities.UserContext;
import io.vertx.core.Future;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;

@ExtendWith(VertxExtension.class)
public class AuthorizationHandlerTests {

    private final AuthorizationHandlerBuilder authzHandlerBuilder = new AuthorizationHandlerBuilder(
            List.of("a", "b"), new TestAuthorizationProvider());

    @Test
    public void testAuthorizationHandler(VertxTestContext testCtx) {
        Checkpoint checks = testCtx.checkpoint(5);

        authzHandlerBuilder
                .build(PermissionAuthorization.of(ResourceAction.TOPIC_CREATE, "{topic}"))
                .authorize(testUser("a", false), Map.of("topic", "t1")::get)
                .onComplete(testCtx.succeeding(v -> checks.flag()));

        authzHandlerBuilder
                .build(PermissionAuthorization.of(ResourceAction.SUBSCRIPTION_DELETE, "{topic}"))
                .authorize(testUser("a", true), Map.of("topic", "t1")::get)
                .onComplete(testCtx.failing(t -> {
                    Assertions.assertEquals(401, ((HttpException) t).getStatusCode());
                    checks.flag();
                }));

        authzHandlerBuilder
                .build(PermissionAuthorization.of(ResourceAction.TOPIC_DELETE, "{topic}"))
                .authorize(testUser("alice", false), Map.of("topic", "t1")::get)
                .onComplete(testCtx.failing(t -> {
                    Assertions.assertEquals(403, ((HttpException) t).getStatusCode());
                    checks.flag();
                }));

        authzHandlerBuilder
                .build(PermissionAuthorization.of(ResourceAction.TOPIC_GET, "{topic}"))
                .authorize(testUser("intern", false), Map.of("topic", "t1")::get)
                .onComplete(testCtx.succeeding(v -> checks.flag()));

        authzHandlerBuilder
                .build(PermissionAuthorization.of(ResourceAction.TOPIC_GET, "{topic}"))
                .authorize(testUser("doom", false), Map.of("topic", "t1")::get)
                .onComplete(testCtx.failing(t -> {
                    Assertions.assertEquals(500, ((HttpException) t).getStatusCode());
                    checks.flag();
                }));
    }

    private UserContext testUser(String name, boolean expired) {
        return new UserContext() {
            @Override
            public String getSubject() {
                return name;
            }

            @Override
            public boolean isExpired() {
                return expired;
            }
        };
    }

    static class TestAuthorizationProvider implements AuthorizationProvider {
        @Override
        public Future<Boolean> init(AuthorizationOptions authorizationOptions) {
            return Future.succeededFuture();
        }

        @Override
        public Future<Boolean> isAuthorized(UserContext userContext, ResourceAction action, String resource) {
            if (List.of("superman", "manager", "architect").contains(userContext.getSubject())) {
                return Future.succeededFuture(true);
            } else if (List.of("alice", "bob", "intern").contains(userContext.getSubject()) &&
                    action.toString().endsWith("get")) {
                return Future.succeededFuture(true);
            } else if (List.of("doom").contains(userContext.getSubject())) {
                return Future.failedFuture(new RuntimeException("it was destined to be doomed"));
            } else {
                return Future.succeededFuture(false);
            }
        }
    }
}
