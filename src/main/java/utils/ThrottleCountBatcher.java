package utils;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class ThrottleCountBatcher<T> {

    private List<T> batchList;
    private int batchCount;

    public ThrottleCountBatcher(int batchCount) {
        this.batchCount = batchCount;
        this.batchList = new ArrayList<>();
    }

    public void append(T event) {
        batchList.add(event);
    }

    public List<List<T>> getBatches() {
        return Lists.partition(this.batchList, batchCount);
    }
}

