package com.flipkart.varadhi.core.cluster;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

/**
 * Live pod count over a started {@link ClusterMembershipView}, filtered by a {@link Predicate} on
 * {@link MemberInfo}.
 * <p>
 * Notifies listeners only when the counted value changes. {@code minCount} floors
 * {@link #getAsInt()} — use {@code 1} for fail-open even-split produce math (VIP §13).
 * <p>
 * The membership view must already be {@linkplain ClusterMembershipView#start() started} before
 * construction; this class does not own that lifecycle.
 */
@Slf4j
public final class PodCountProvider implements IntSupplier {

    private static final Predicate<MemberInfo> ALL_MEMBERS = ignored -> true;

    private final ClusterMembershipView membership;
    private final Predicate<MemberInfo> memberFilter;
    private final int minCount;
    private final CopyOnWriteArrayList<Runnable> countChangeListeners = new CopyOnWriteArrayList<>();
    private volatile int podCount;

    private PodCountProvider(ClusterMembershipView membership, Predicate<MemberInfo> memberFilter, int minCount) {
        this.membership = membership;
        this.memberFilter = memberFilter;
        this.minCount = minCount;
        this.podCount = minCount;
        membership.addMembershipChangeListener(this::updateCountIfChanged);
        updateCountIfChanged();
    }

    /** Count every cluster member (all roles). */
    public static PodCountProvider all(ClusterMembershipView membership) {
        return all(membership, 0);
    }

    public static PodCountProvider all(ClusterMembershipView membership, int minCount) {
        return new PodCountProvider(membership, ALL_MEMBERS, minCount);
    }

    public static PodCountProvider withRole(ClusterMembershipView membership, ComponentKind role) {
        return withRole(membership, role, 0);
    }

    public static PodCountProvider withRole(ClusterMembershipView membership, ComponentKind role, int minCount) {
        return filtered(membership, member -> member.hasRole(role), minCount);
    }

    public static PodCountProvider filtered(ClusterMembershipView membership, Predicate<MemberInfo> memberFilter) {
        return filtered(membership, memberFilter, 0);
    }

    public static PodCountProvider filtered(
        ClusterMembershipView membership,
        Predicate<MemberInfo> memberFilter,
        int minCount
    ) {
        return new PodCountProvider(membership, memberFilter, minCount);
    }

    @Override
    public int getAsInt() {
        return podCount;
    }

    public void addCountChangeListener(Runnable listener) {
        countChangeListeners.add(listener);
    }

    private void updateCountIfChanged() {
        int newCount = Math.max(minCount, countMatchingMembers());
        int previous = podCount;
        if (newCount != previous) {
            podCount = newCount;
            log.info("Pod count changed from {} to {}", previous, newCount);
            countChangeListeners.forEach(Runnable::run);
        }
    }

    private int countMatchingMembers() {
        int count = 0;
        for (MemberInfo member : membership.snapshot().values()) {
            if (memberFilter.test(member)) {
                count++;
            }
        }
        return count;
    }
}
