package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.core.RegionService;
import com.flipkart.varadhi.entities.Region;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.web.RegionCreateRequest;
import com.flipkart.varadhi.entities.web.RegionPathParams;
import com.flipkart.varadhi.entities.web.RegionStatusUpdateRequest;
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
 * HTTP handlers for region metadata (list/get/create/update status/delete).
 * <p>
 * {@code :region} path segments are validated with the same naming rules as create bodies; invalid names return HTTP 400.
 * A valid name that does not exist returns HTTP 404 from the metastore.
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
                               .bodyParser(this::setRegionCreate)
                               .authorize(REGION_CREATE)
                               .build(this::getHierarchies, this::create),
                RouteDefinition.patch(UPDATE, API_NAME, "/:region")
                               .hasBody()
                               .bodyParser(this::setRegionStatusUpdate)
                               .authorize(REGION_UPDATE)
                               .build(this::getHierarchies, this::patchStatus),
                RouteDefinition.delete(DELETE, API_NAME, "/:region")
                               .authorize(REGION_DELETE)
                               .build(this::getHierarchies, this::delete)
            )
        ).get();
    }

    public void setRegionCreate(RoutingContext ctx) {
        RegionCreateRequest req = ctx.body().asValidatedPojo(RegionCreateRequest.class);
        ctx.put(REQUEST_BODY, req);
    }

    public void setRegionStatusUpdate(RoutingContext ctx) {
        RegionStatusUpdateRequest req = ctx.body().asValidatedPojo(RegionStatusUpdateRequest.class);
        ctx.put(REQUEST_BODY, req);
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        String regionName = ctx.pathParam(PATH_PARAM_REGION);
        if (regionName != null && !regionName.isEmpty()) {
            return Map.of(ResourceType.REGION, new Hierarchies.RegionHierarchy(regionName));
        }
        if (hasBody) {
            Object body = ctx.get(REQUEST_BODY);
            if (body instanceof RegionCreateRequest req) {
                return Map.of(ResourceType.REGION, new Hierarchies.RegionHierarchy(req.name()));
            }
        }
        return Map.of(ResourceType.ROOT, new Hierarchies.RootHierarchy());
    }

    public void list(RoutingContext ctx) {
        ctx.endApiWithResponse(regionService.getRegions());
    }

    public void get(RoutingContext ctx) {
        String regionName = ctx.pathParam(PATH_PARAM_REGION);
        RegionPathParams.validateRegionName(regionName);
        ctx.endApiWithResponse(regionService.getRegion(regionName));
    }

    public void create(RoutingContext ctx) {
        RegionCreateRequest req = ctx.get(REQUEST_BODY);
        Region created = regionService.createRegion(req);
        ctx.endApiWithResponse(created);
    }

    public void patchStatus(RoutingContext ctx) {
        String regionName = ctx.pathParam(PATH_PARAM_REGION);
        RegionPathParams.validateRegionName(regionName);
        RegionStatusUpdateRequest req = ctx.get(REQUEST_BODY);
        Region updated = regionService.updateRegionStatus(regionName, req.status());
        ctx.endApiWithResponse(updated);
    }

    public void delete(RoutingContext ctx) {
        String regionName = ctx.pathParam(PATH_PARAM_REGION);
        RegionPathParams.validateRegionName(regionName);
        regionService.deleteRegion(regionName);
        ctx.endApi();
    }
}
