package com.flipkart.varadhi.core.cluster.entities;

import com.flipkart.varadhi.entities.cluster.Assignment;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShardAssignments {
    private List<Assignment> assignments;
}
