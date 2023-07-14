package com.flipkart.varadhi.web.v1;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.db.MetaStore;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.services.OrgService;
import com.flipkart.varadhi.services.VaradhiEntityService;
import com.flipkart.varadhi.services.VaradhiEntityServiceFactory;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.utils.VaradhiEntityUtils;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.flipkart.varadhi.auth.ResourceAction.*;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.hasBody;

@Slf4j
@ExtensionMethod({RequestBodyExtension.class, RoutingContextExtension.class})
public class VaradhiEntityHandlers implements RouteProvider {

    private final VaradhiEntityServiceFactory varadhiEntityServiceFactory;

    public VaradhiEntityHandlers(VaradhiEntityServiceFactory varadhiEntityServiceFactory) {

        this.varadhiEntityServiceFactory = varadhiEntityServiceFactory;

    }

    @Override
    public List<RouteDefinition> get() {
        //TODO : implement this for each entity type
        return new SubRoutes(
                "/v1/tenants/:tenant/orgs",
                List.of(
                        new RouteDefinition(
                                HttpMethod.POST, "", Set.of(authenticated, hasBody), this::createOrg,
                                Optional.of(PermissionAuthorization.of(ORG_CREATE, "{tenant}"))
                        )
                )
        ).get();
    }

    public void get(RoutingContext ctx) {
        ctx.todo();
    }

    public void createOrg(RoutingContext ctx) {
        create(ctx, VaradhiEntityType.ORG);
    }

    public void create(RoutingContext ctx, VaradhiEntityType varadhiEntityType) {
        //TODO:: Enable authn/authz for this flow.
        //TODO:: Consider using Vertx ValidationHandlers to validate the request body.
        //TODO:: Consider reverting on failure and transaction kind of semantics for all operations.

        VaradhiEntity varadhiEntity = ctx.body().asPojo(VaradhiEntityUtils.fetchEntityClassFromType(varadhiEntityType));

        VaradhiEntityService varadhiEntityService = varadhiEntityServiceFactory.getVaradhiEntityService(varadhiEntityType);

        varadhiEntity = varadhiEntityService.create(varadhiEntity);

        ctx.endRequestWithResponse(varadhiEntity);
    }


    public void delete(RoutingContext ctx) {
        ctx.todo();
    }
}
