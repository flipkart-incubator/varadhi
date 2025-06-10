package com.flipkart.varadhi.web.core.authz;

import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.common.FutureExtensions;
import com.flipkart.varadhi.web.spi.authz.AuthorizationProvider;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.VertxUserContext;
import com.flipkart.varadhi.entities.auth.ResourceAction;
import com.flipkart.varadhi.entities.auth.UserContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import lombok.AllArgsConstructor;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;

import static java.net.HttpURLConnection.*;

@Slf4j
@ExtensionMethod ({FutureExtensions.class})
public class AuthorizationHandlerBuilder {

    private final AuthorizationProvider provider;
    private final Timer timer;

    private final MeterRegistry meterRegistry;

    public AuthorizationHandlerBuilder(AuthorizationProvider provider, MeterRegistry meterRegistry) {
        this.provider = Objects.requireNonNull(provider, "Authorization Provider is null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "Meter registry is null");
        this.timer = Timer.builder("authorization.timer")
                          .description("Time taken to check user authorization")
                          .tag("method", "isAuthorized")
                          .register(this.meterRegistry);
    }

    public AuthorizationHandler build(ResourceAction authorizationOnResource) {
        return new AuthorizationHandler(authorizationOnResource);
    }

    private Future<Void> authorizedInternal(UserContext userContext, ResourceAction action, String resourcePath) {
        Timer.Sample clock = Timer.start(meterRegistry);

        return provider.isAuthorized(userContext, action, resourcePath).andThen(ar -> {
            if (ar.succeeded()) {
                Boolean authorized = ar.result();
                if (Boolean.FALSE.equals(authorized)) {
                    throw new HttpException(
                        HTTP_FORBIDDEN,
                        String.format(
                            "User %s is not authorized to perform action %s on resource %s",
                            userContext.getSubject(),
                            action,
                            resourcePath
                        )
                    );
                }
            } else {
                throw new HttpException(HTTP_INTERNAL_ERROR, "failed to get user authorization");
            }
        }).record(clock, timer).mapEmpty();
    }

    @AllArgsConstructor
    public class AuthorizationHandler implements Handler<RoutingContext> {

        private final ResourceAction authorizationOnAction;

        @Override
        public void handle(RoutingContext ctx) {
            UserContext user = ctx.user() == null ? null : new VertxUserContext(ctx.user());
            Map<ResourceType, ResourceHierarchy> hierarchies = ctx.get(Constants.ContextKeys.RESOURCE_HIERARCHY);
            ResourceHierarchy hierarchy = hierarchies.getOrDefault(authorizationOnAction.getResourceType(), null);
            if (null == hierarchy) {
                ctx.fail(new HttpException(HTTP_INTERNAL_ERROR, "resource hierarchy is not set."));
                return;
            }
            authorize(user, hierarchy).onFailure(ctx::fail).onComplete(result -> ctx.next());
        }


        Future<Void> authorize(UserContext userContext, ResourceHierarchy resourceHierarchy) {

            if (userContext == null) {
                return Future.failedFuture(new HttpException(HTTP_UNAUTHORIZED, "the request is not authenticated"));
            }

            if (userContext.isExpired()) {
                return Future.failedFuture(new HttpException(HTTP_UNAUTHORIZED, "the user / token has been expired"));
            }

            if (resourceHierarchy == null) {
                return Future.failedFuture(new HttpException(HTTP_INTERNAL_ERROR, "resource hierarchy is not set"));
            }

            String resourcePath = resourceHierarchy.getResourcePath();
            return authorizedInternal(userContext, authorizationOnAction, resourcePath);
        }
    }
}
