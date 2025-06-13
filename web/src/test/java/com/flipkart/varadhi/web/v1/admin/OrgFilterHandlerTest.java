package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.entities.filters.StringConditions;
import com.flipkart.varadhi.core.OrgService;
import com.flipkart.varadhi.entities.web.ErrorResponse;
import com.flipkart.varadhi.web.WebTestBase;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class OrgFilterHandlerTest extends WebTestBase {
    OrgFilterHandler orgFilterHandler;
    OrgService orgService;
    String basePath = "/v1/orgs";

    @BeforeEach
    public void setUpTest() throws InterruptedException {
        super.setUp();
        orgService = mock(OrgService.class);
        orgFilterHandler = new OrgFilterHandler(orgService);

        // Register filter endpoints under /v1/orgs/:org/filters
        Route routeGetFilters = router.get("/v1/orgs/:org/filters").handler(wrapBlocking(orgFilterHandler::list));
        setupFailureHandler(routeGetFilters);
        Route routeGetFilterByName = router.get("/v1/orgs/:org/filters/:orgFilterName")
                                           .handler(wrapBlocking(orgFilterHandler::get));
        setupFailureHandler(routeGetFilterByName);
        Route routeCreateFilter = router.post("/v1/orgs/:org/filters").handler(bodyHandler).handler(ctx -> {
            orgFilterHandler.setNamedFilter(ctx);
            ctx.next();
        }).handler(wrapBlocking(orgFilterHandler::create));
        setupFailureHandler(routeCreateFilter);
        Route routeUpdateFilter = router.put("/v1/orgs/:org/filters/:orgFilterName")
                                        .handler(bodyHandler)
                                        .handler(ctx -> {
                                            orgFilterHandler.setNamedFilter(ctx);
                                            ctx.next();
                                        })
                                        .handler(wrapBlocking(orgFilterHandler::update));
        setupFailureHandler(routeUpdateFilter);
    }

    private String getFilterBaseUrl(String org) {
        return String.join("/", basePath, org, "filters");
    }

    private String getFilterByNameUrl(String org, String filterName) {
        return String.join("/", basePath, org, "filters", filterName);
    }

    @AfterEach
    public void tearDownTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    public void testList() throws Exception {
        String orgName = "org1";
        Map<String, Condition> conditionMap = new HashMap<>();
        conditionMap.put("filterA", new StringConditions.ExistsCondition("X_abc"));
        conditionMap.put("nameGroup.filterName", new StringConditions.ExistsCondition("X_abc"));
        OrgFilters orgFilter = new OrgFilters(0, conditionMap);
        // Assume filters are set in the service
        when(orgService.getAllFilters(eq(orgName))).thenReturn(orgFilter);

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getFilterBaseUrl(orgName));
        OrgFilters obtainedFilters = sendRequestWithoutPayload(request, WebTestBase.c(OrgFilters.class));
        Assertions.assertEquals(orgFilter, obtainedFilters);
        verify(orgService, times(1)).getAllFilters(eq(orgName));

        // Test not found scenario
        when(orgService.getAllFilters(eq(orgName))).thenThrow(
            new ResourceNotFoundException("Org(" + orgName + ") filter not found.")
        );
        sendRequestWithoutPayload(request, 404, "Org(" + orgName + ") filter not found.");
    }

    @Test
    public void testGet() throws Exception {
        String orgName = "org1";
        String filterName = "myFilter";
        Condition condition = new StringConditions.ContainsCondition("test", "x_abc");
        when(orgService.getFilter(eq(orgName), eq(filterName))).thenReturn(condition);

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getFilterByNameUrl(orgName, filterName));
        Condition obtained = sendRequestWithoutPayload(request, WebTestBase.c(Condition.class));
        Assertions.assertEquals(condition, obtained);
        verify(orgService, times(1)).getFilter(eq(orgName), eq(filterName));

        // Negative scenario: filter not found
        String errorMsg = "Filter not found.";
        when(orgService.getFilter(eq(orgName), eq(filterName))).thenThrow(new ResourceNotFoundException(errorMsg));
        sendRequestWithoutPayload(request, 404, errorMsg);
    }

    @Test
    public void testCreate() throws Exception {
        String orgName = "org1";
        Map<String, Condition> conditionMap = new HashMap<>();
        conditionMap.put("filterA", new StringConditions.ExistsCondition("X_abc"));
        conditionMap.put("nameGroup.filterName", new StringConditions.ExistsCondition("X_abc"));
        OrgFilters inputFilters = new OrgFilters(0, conditionMap);
        OrgFilters createdFilters = new OrgFilters(0, conditionMap);


        when(orgService.createFilter(eq(orgName), eq(inputFilters))).thenReturn(createdFilters);

        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getFilterBaseUrl(orgName));
        OrgFilters obtained = sendRequestWithEntity(request, inputFilters, WebTestBase.c(OrgFilters.class));
        Assertions.assertEquals(createdFilters, obtained);
        verify(orgService, times(1)).createFilter(eq(orgName), eq(inputFilters));

        // Error case: creation fails
        String errorMsg = "Failed to create filter.";
        when(orgService.createFilter(eq(orgName), eq(inputFilters))).thenThrow(new ResourceNotFoundException(errorMsg));
        ErrorResponse response = sendRequestWithEntity(
            request,
            inputFilters,
            404,
            errorMsg,
            WebTestBase.c(ErrorResponse.class)
        );
        Assertions.assertEquals(errorMsg, response.reason());
    }

    @Test
    public void testUpdate() throws Exception {
        String orgName = "org1";
        String filterName = "nameGroup.filterName";

        Map<String, Condition> conditionMap = new HashMap<>();
        conditionMap.put("filterA", new StringConditions.ExistsCondition("X_abc"));
        OrgFilters inputFilters = new OrgFilters(0, conditionMap);

        // assume update does not return any entity
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, getFilterByNameUrl(orgName, filterName));
        sendRequestWithEntity(request, inputFilters, null);
        verify(orgService, times(1)).updateFilter(eq(orgName), eq(inputFilters));
    }
}
