package com.bonree.brfs.client;

import com.google.common.collect.ImmutableList;
import java.util.List;

public interface BatchResult {

    List<PutObjectResult> getResults();

    static BatchResult EMPTY = new BatchResult() {
        @Override
        public List<PutObjectResult> getResults() {
            return ImmutableList.of();
        }
    };

    static BatchResult from(List<String> fids) {
        List<PutObjectResult> results = fids.stream()
            .map(PutObjectResult::of)
            .collect(ImmutableList.toImmutableList());

        return () -> results;
    }
}
