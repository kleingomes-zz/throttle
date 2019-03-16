package utils;

import java.util.ArrayList;
import java.util.List;

public class ThrottleSizeByteBatcher {

    private List<byte[]> batchList;
    private int sizeBytes;

    public ThrottleSizeByteBatcher(int sizeBytes) {
        this.sizeBytes = sizeBytes;
        this.batchList = new ArrayList<>();
    }

    public void append(byte[] event) {
        batchList.add(event);
    }

    public List<List<byte[]>> getBatches() {
        int accumulator = 0;
        List<List<byte[]>> result = new ArrayList<>();
        List<byte[]> temp = new ArrayList<>();
        for (byte[] event : this.batchList) {
            int size = event.length;
            if ((accumulator + size) <= sizeBytes) {
                accumulator += size;
                temp.add(event);
            } else {
                result.add(copy(temp));
                temp.clear();
                accumulator = 0;
            }
        }
        temp.clear();
        return result;
    }

    private List<byte[]> copy(List<byte[]> source) {
        List<byte[]> result = new ArrayList<>();
        source.forEach(result::add);
        return result;
    }
}

