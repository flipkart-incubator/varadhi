package com.flipkart.varadhi.entities.cluster;

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
