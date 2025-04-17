package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.entities.Hierarchies;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.OrgFilters;
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
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_ORG;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_ORG_FILTER_NAME;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;

@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class OrgFilterHandler implements RouteProvider {
    private final OrgService orgService;

    public OrgFilterHandler(OrgService orgService) {
        this.orgService = orgService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/orgs/:org/filters",
            List.of(
                RouteDefinition.get("GetFilters", "").authorize(ORG_GET).build(this::getHierarchies, this::getAll),
                RouteDefinition.get("GetFilterByName", "/:orgFilterName")
                               .authorize(ORG_GET)
                               .build(this::getHierarchies, this::get),
                RouteDefinition.post("CreateFilter", "")
                               .hasBody()
                               .bodyParser(this::setNamedFilter)
                               .authorize(ORG_CREATE)
                               .build(this::getHierarchies, this::create),
                RouteDefinition.put("UpdateFilter", "")
                               .hasBody()
                               .bodyParser(this::setNamedFilter)
                               .authorize(ORG_UPDATE)
                               .build(this::getHierarchies, this::update),
                RouteDefinition.delete("deleteFilter", "")
                               .authorize(ORG_DELETE)
                               .build(this::getHierarchies, this::delete)
            )
        ).get();
    }

    public void setNamedFilter(RoutingContext ctx) {
        OrgFilters namedFilter = ctx.body().asPojo(OrgFilters.class);
        ctx.put(CONTEXT_KEY_BODY, namedFilter);
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        String orgName = ctx.request().getParam(PATH_PARAM_ORG);
        return Map.of(ResourceType.ORG, new Hierarchies.OrgHierarchy(orgName));
    }

    public void getAll(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        OrgFilters filters = orgService.getAllFilters(orgName);
        ctx.endApiWithResponse(filters);
    }

    public void get(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        String filterName = ctx.pathParam(PATH_PARAM_ORG_FILTER_NAME);
        Condition filter = orgService.getFilter(orgName, filterName);
        ctx.endApiWithResponse(filter);
    }

    public void create(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        OrgFilters filter = ctx.get(CONTEXT_KEY_BODY);
        OrgFilters createdFilter = orgService.createFilter(orgName, filter);
        ctx.endApiWithResponse(createdFilter);
    }

    public void update(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        OrgFilters filter = ctx.get(CONTEXT_KEY_BODY);
        orgService.updateFilter(orgName, filter);
        ctx.endApi();
    }

    public void delete(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        orgService.deleteFilter(orgName);
        ctx.endApi();
    }
}
