package com.flipkart.varadhi.web.v1;

import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.services.OrgService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import com.google.common.collect.Sets;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.flipkart.varadhi.auth.ResourceAction.ORG_DELETE;
import static com.flipkart.varadhi.auth.ResourceAction.ORG_GET;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.hasBody;


@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class OrgHandlers implements RouteProvider {
    //TODO:: Discuss case preserving or lower case requirement and close this again.
    private final OrgService orgService;

    public OrgHandlers(OrgService orgService) {
        this.orgService = orgService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/orgs",
                List.of(
                        new RouteDefinition(
                                HttpMethod.GET, "", Set.of(authenticated),
                                Sets.newLinkedHashSet(), this::getOrgs, true, Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.GET, "/:org", Set.of(authenticated),
                                Sets.newLinkedHashSet(),this::get,true,
                                Optional.of(PermissionAuthorization.of(ORG_GET, "{org}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.POST, "", Set.of(hasBody),
                                Sets.newLinkedHashSet(),this::create,true, Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE, "/:org", Set.of(authenticated),
                                Sets.newLinkedHashSet(),this::delete, true,
                                Optional.of(PermissionAuthorization.of(ORG_DELETE, "{org}"))
                        )
                )
        ).get();
    }

    public void getOrgs(RoutingContext ctx) {
        List<Org> orgs = this.orgService.getOrgs();
        ctx.endRequestWithResponse(orgs);
    }

    public void get(RoutingContext ctx) {
        String orgName = ctx.pathParam(Constants.PathParams.ORG_PATH_PARAM);
        Org org = this.orgService.getOrg(orgName);
        ctx.endRequestWithResponse(org);
    }

    public void create(RoutingContext ctx) {
        //TODO:: Authz check need to be explicit here. This can be done with Authz work.
        Org org = ctx.body().asPojo(Org.class);
        Org createdorg = this.orgService.createOrg(org);
        ctx.endRequestWithResponse(createdorg);
    }

    public void delete(RoutingContext ctx) {
        String orgName = ctx.pathParam(Constants.PathParams.ORG_PATH_PARAM);
        this.orgService.deleteOrg(orgName);
        ctx.endRequest();
    }
}
