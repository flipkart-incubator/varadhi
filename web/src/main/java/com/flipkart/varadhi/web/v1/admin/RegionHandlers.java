package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.core.RegionService;
import com.flipkart.varadhi.entities.Region;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.hierarchy.Hierarchies;
import com.flipkart.varadhi.web.hierarchy.ResourceHierarchy;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.ContextKeys.REQUEST_BODY;
import static com.flipkart.varadhi.common.Constants.MethodNames.*;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_REGION;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;

/**
 * HTTP handlers for region metadata (list/get/create/delete).
 */
@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class RegionHandlers implements RouteProvider {

    private static final String API_NAME = "REGION";

    private final RegionService regionService;

    public RegionHandlers(RegionService regionService) {
        this.regionService = regionService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/regions",
            List.of(
                RouteDefinition.get(LIST, API_NAME, "").authorize(REGION_LIST).build(this::getHierarchies, this::list),
                RouteDefinition.get(GET, API_NAME, "/:region")
                               .authorize(REGION_GET)
                               .build(this::getHierarchies, this::get),
                RouteDefinition.post(CREATE, API_NAME, "")
                               .hasBody()
                               .bodyParser(this::setRegion)
                               .authorize(REGION_CREATE)
                               .build(this::getHierarchies, this::create),
                RouteDefinition.delete(DELETE, API_NAME, "/:region")
                               .authorize(REGION_DELETE)
                               .build(this::getHierarchies, this::delete)
            )
        ).get();
    }

    public void setRegion(RoutingContext ctx) {
        Region region = ctx.body().asValidatedPojo(Region.class);
        ctx.put(REQUEST_BODY, region);
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        if (hasBody) {
            Region region = ctx.get(REQUEST_BODY);
            return Map.of(ResourceType.REGION, new Hierarchies.RegionHierarchy(region.getName()));
        }
        String regionName = ctx.pathParam(PATH_PARAM_REGION);
        if (regionName == null) {
            return Map.of(ResourceType.ROOT, new Hierarchies.RootHierarchy());
        }
        return Map.of(ResourceType.REGION, new Hierarchies.RegionHierarchy(regionName));
    }

    public void list(RoutingContext ctx) {
        ctx.endApiWithResponse(regionService.getRegions());
    }

    public void get(RoutingContext ctx) {
        String regionName = ctx.pathParam(PATH_PARAM_REGION);
        ctx.endApiWithResponse(regionService.getRegion(regionName));
    }

    public void create(RoutingContext ctx) {
        Region region = ctx.get(REQUEST_BODY);
        Region created = regionService.createRegion(region);
        ctx.endApiWithResponse(created);
    }

    public void delete(RoutingContext ctx) {
        String regionName = ctx.pathParam(PATH_PARAM_REGION);
        regionService.deleteRegion(regionName);
        ctx.endApi();
    }
}
