package com.flipkart.varadhi.web.v1.admin;


import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.services.OrgService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_ORG;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;


@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class OrgHandlers implements RouteProvider {
    private final OrgService orgService;

    public OrgHandlers(OrgService orgService) {
        this.orgService = orgService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/orgs",
                List.of(
                        RouteDefinition.get("")
                                .blocking().authenticatedWith(PermissionAuthorization.of(ORG_LIST, ""))
                                .build(this::getOrganizations),
                        RouteDefinition.get("/:org")
                                .blocking().authenticatedWith(PermissionAuthorization.of(ORG_GET, "{org}"))
                                .build(this::get),
                        RouteDefinition.post("")
                                .blocking().hasBody().authenticatedWith(PermissionAuthorization.of(ORG_CREATE, ""))
                                .build(this::create),
                        RouteDefinition.delete("/:org")
                                .blocking().authenticatedWith(PermissionAuthorization.of(ORG_DELETE, "{org}"))
                                .build(this::delete)
                )
        ).get();
    }

    public void getOrganizations(RoutingContext ctx) {
        List<Org> organizations = orgService.getOrgs();
        ctx.endApiWithResponse(organizations);
    }

    public void get(RoutingContext ctx) {
        String orgName = ctx.pathParam(REQUEST_PATH_PARAM_ORG);
        Org org = orgService.getOrg(orgName);
        ctx.endApiWithResponse(org);
    }

    public void create(RoutingContext ctx) {
        Org org = ctx.body().asValidatedPojo(Org.class);
        Org createdorg = orgService.createOrg(org);
        ctx.endApiWithResponse(createdorg);
    }

    public void delete(RoutingContext ctx) {
        String orgName = ctx.pathParam(REQUEST_PATH_PARAM_ORG);
        orgService.deleteOrg(orgName);
        ctx.endApi();
    }
}
