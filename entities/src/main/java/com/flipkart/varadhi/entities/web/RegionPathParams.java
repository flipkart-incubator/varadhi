package com.flipkart.varadhi.entities.web;

import com.flipkart.varadhi.entities.Region;
import com.flipkart.varadhi.entities.RegionStatus;

/**
 * Validates {@code :region} path segments for region APIs.
 * <p>
 * Uses the same naming rules as {@link Region} / {@link com.flipkart.varadhi.entities.ValidateResource} as POST
 * create bodies. Invalid names yield HTTP 400 (validation failure). A syntactically valid name that does not exist
 * in the metastore still yields HTTP 404 from the store layer.
 */
public final class RegionPathParams {

    private RegionPathParams() {
    }

    /**
     * Validates the region id segment; throws {@link IllegalArgumentException} if the name is invalid (same as body validation).
     */
    public static void validateRegionName(String regionName) {
        new Region(regionName, Region.INITIAL_VERSION, RegionStatus.AVAILABLE).validate();
    }
}
