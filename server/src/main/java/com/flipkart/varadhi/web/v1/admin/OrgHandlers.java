package com.flipkart.varadhi.web.v1.admin;


import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.services.OrgService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_ORG;
import static com.flipkart.varadhi.auth.ResourceAction.ORG_DELETE;
import static com.flipkart.varadhi.auth.ResourceAction.ORG_GET;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.hasBody;


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
                        new RouteDefinition(
                                HttpMethod.GET,
                                "",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::getOrganizations,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/:org",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::get,
                                true,
                                Optional.of(PermissionAuthorization.of(ORG_GET, "{org}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.POST,
                                "",
                                Set.of(hasBody),
                                new LinkedHashSet<>(),
                                this::create,
                                true,
                                Optional.empty()
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE,
                                "/:org",
                                Set.of(authenticated),
                                new LinkedHashSet<>(),
                                this::delete,
                                true,
                                Optional.of(PermissionAuthorization.of(ORG_DELETE, "{org}"))
                        )
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
        //TODO:: Authz check need to be explicit here. This can be done with Authz work.
        Org org = ctx.body().asValidatedPojo(Org.class);
        Org createdorg = orgService.createOrg(org.cloneForCreate());
        ctx.endApiWithResponse(createdorg);
    }

    public void delete(RoutingContext ctx) {
        String orgName = ctx.pathParam(REQUEST_PATH_PARAM_ORG);
        orgService.deleteOrg(orgName);
        ctx.endApi();
    }
}
