package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.services.NamedFilterService;
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

@Slf4j
@ExtensionMethod({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class NamedFilterHandlers implements RouteProvider {
    private final NamedFilterService namedFilterService;

    public NamedFilterHandlers(NamedFilterService namedFilterService) {
        this.namedFilterService = namedFilterService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/entities/ORG/:orgName/globalFilters",
                List.of(
                        RouteDefinition.get("GetGlobalFilters", "")
                                .authorize(FILTER_LIST)
                                .build(this::getHierarchies, this::getGlobalFilters),
                        RouteDefinition.put("ReplaceGlobalFilters", "")
                                .hasBody()
                                .bodyParser(this::setNamedFilters)
                                .authorize(FILTER_REPLACE)
                                .build(this::getHierarchies, this::replaceGlobalFilters),
                        RouteDefinition.post("AddOrUpdateGlobalFilter", "")
                                .hasBody()
                                .bodyParser(this::setNamedFilter)
                                .authorize(FILTER_CREATE_OR_UPDATE)
                                .build(this::getHierarchies, this::addOrUpdateGlobalFilter),
                        RouteDefinition.patch("UpdateGlobalFilter", "")
                                .hasBody()
                                .bodyParser(this::setNamedFilter)
                                .authorize(FILTER_UPDATE)
                                .build(this::getHierarchies, this::updateGlobalFilter),
                        RouteDefinition.delete("DeleteGlobalFilters", "")
                                .authorize(FILTER_DELETE)
                                .build(this::getHierarchies, this::deleteGlobalFilters)
                )
        ).get();
    }

    public void setNamedFilters(RoutingContext ctx) {
        OrgFilters orgFilters = ctx.body().asPojo(OrgFilters.class);
        ctx.put(CONTEXT_KEY_BODY, orgFilters);
    }

    public void setNamedFilter(RoutingContext ctx) {
        OrgFilters orgFilters = ctx.body().asPojo(OrgFilters.class);
        ctx.put(CONTEXT_KEY_BODY, namedFilter);
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        String orgName = ctx.request().getParam(PATH_PARAM_ORG);
        return Map.of(ResourceType.ORG, new Hierarchies.OrgHierarchy(orgName));
    }

    public void getGlobalFilters(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        List<OrgFilters> filters = namedFilterService.getAllGlobalFilters(orgName);
        ctx.endApiWithResponse(filters);
    }

    public void replaceGlobalFilters(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        List<OrgFilters> filters = ctx.get(CONTEXT_KEY_BODY);
        namedFilterService.replaceGlobalFilters(orgName, filters);
        ctx.endApi();
    }

    public void addOrUpdateGlobalFilter(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        OrgFilters filter = ctx.get(CONTEXT_KEY_BODY);
        namedFilterService.addOrUpdateGlobalFilter(orgName, filter);
        ctx.endApi();
    }

    public void updateGlobalFilter(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        OrgFilters filter = ctx.get(CONTEXT_KEY_BODY);
        namedFilterService.updateGlobalFilter(orgName, filter);
        ctx.endApi();
    }

    public void deleteGlobalFilters(RoutingContext ctx) {
        String orgName = ctx.pathParam(PATH_PARAM_ORG);
        namedFilterService.deleteGlobalFilters(orgName);
        ctx.endApi();
    }
}