package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.entities.Hierarchies;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.services.OrgFilterService;
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
import static com.flipkart.varadhi.entities.auth.ResourceAction.ORG_GET;

@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class OrgLevelFilterHandler implements RouteProvider {
    private final OrgFilterService orgFilterService;

    public OrgLevelFilterHandler(OrgFilterService orgFilterService) {
        this.orgFilterService = orgFilterService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/orgs/:org/filters",
            List.of(
                RouteDefinition.get("GetFilters", "")
                               .authorize(ORG_GET)
                               .build(this::getHierarchies, this::getOrgFilters),
                RouteDefinition.get("GetFilterByName", "/:orgFilterName")
                               .authorize(ORG_GET)
                               .build(this::getHierarchies, this::getNamedFilterByName),
                RouteDefinition.post("CreateFilter", "")
                               .hasBody()
                               .bodyParser(this::setNamedFilter)
                               .authorize(ORG_GET)
                               .build(this::getHierarchies, this::createNamedFilter),
                RouteDefinition.put("UpdateFilter", "/:orgFilterName")
                               .hasBody()
                               .bodyParser(this::setNamedFilter)
                               .authorize(ORG_GET)
                               .build(this::getHierarchies, this::updateNamedFilter),
                RouteDefinition.get("CheckFilterExists", "/:orgFilterName/exists")
                               .authorize(ORG_GET)
                               .build(this::getHierarchies, this::checkIfNamedFilterExists)
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

    public void getOrgFilters(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        List<OrgFilters> filters = orgFilterService.getAll(orgName);
        ctx.endApiWithResponse(filters);
    }

    public void getNamedFilterByName(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        String filterName = ctx.pathParam(PATH_PARAM_ORG_FILTER_NAME);
        Condition filter = orgFilterService.getFilter(orgName, filterName);
        ctx.endApiWithResponse(filter);
    }

    public void createNamedFilter(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        OrgFilters filter = ctx.get(CONTEXT_KEY_BODY);
        OrgFilters createdFilter = orgFilterService.createFilter(orgName, filter);
        ctx.endApiWithResponse(createdFilter);
    }

    public void updateNamedFilter(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        String filterName = ctx.pathParam(PATH_PARAM_ORG_FILTER_NAME);
        OrgFilters filter = ctx.get(CONTEXT_KEY_BODY);
        orgFilterService.updateFilter(orgName, filterName, filter);
        ctx.endApi();
    }

    public void checkIfNamedFilterExists(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        String filterName = ctx.pathParam(PATH_PARAM_ORG_FILTER_NAME);
        boolean exists = orgFilterService.filterExists(orgName, filterName);
        ctx.endApiWithResponse(exists);
    }
}
