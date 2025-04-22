package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.entities.Hierarchies;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.services.OrgService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.CONTEXT_KEY_BODY;
import static com.flipkart.varadhi.common.Constants.MethodNames.*;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_ORG;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;


@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class OrgHandlers implements RouteProvider {
    private static final String API_NAME = "ORG";
    private final OrgService orgService;

    public OrgHandlers(OrgService orgService) {
        this.orgService = orgService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/orgs",
            List.of(
                RouteDefinition.get(LIST, API_NAME, "").authorize(ORG_LIST).build(this::getHierarchies, this::list),
                RouteDefinition.get(GET, API_NAME, "/:org").authorize(ORG_GET).build(this::getHierarchies, this::get),
                RouteDefinition.post(CREATE, API_NAME, "")
                               .hasBody()
                               .bodyParser(this::setOrg)
                               .authorize(ORG_CREATE)
                               .build(this::getHierarchies, this::create),
                RouteDefinition.delete(DELETE, API_NAME, "/:org")
                               .authorize(ORG_DELETE)
                               .build(this::getHierarchies, this::delete)
            )
        ).get();
    }

    public void setOrg(RoutingContext ctx) {
        Org org = ctx.body().asValidatedPojo(Org.class);
        ctx.put(CONTEXT_KEY_BODY, org);
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        if (hasBody) {
            Org org = ctx.get(CONTEXT_KEY_BODY);
            return Map.of(ResourceType.ORG, new Hierarchies.OrgHierarchy(org.getName()));
        }
        String org = ctx.request().getParam(PATH_PARAM_ORG);
        if (org == null) {
            return Map.of(ResourceType.ROOT, new Hierarchies.RootHierarchy());
        }
        return Map.of(ResourceType.ORG, new Hierarchies.OrgHierarchy(org));
    }


    public void list(RoutingContext ctx) {
        List<Org> organizations = orgService.getOrgs();
        ctx.endApiWithResponse(organizations);
    }

    public void get(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        Org org = orgService.getOrg(orgName);
        ctx.endApiWithResponse(org);
    }

    public void create(RoutingContext ctx) {
        Org org = ctx.get(CONTEXT_KEY_BODY);
        Org createdorg = orgService.createOrg(org);
        ctx.endApiWithResponse(createdorg);
    }

    public void delete(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        orgService.deleteOrg(orgName);
        ctx.endApi();
    }
}
