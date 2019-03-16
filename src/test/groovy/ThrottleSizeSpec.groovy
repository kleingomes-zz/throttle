import spock.lang.Specification
import spock.lang.Unroll
import utils.ThrottleSizeByteBatcher

import java.util.stream.IntStream

class ThrottleSizeSpec extends Specification {

    ThrottleSizeByteBatcher throttleSizeBatcher

    @Unroll
    void 'it should send #totalEvents events at a rate of #eps events per second and complete in #expectedTotalTime seconds'() {
        given:
        throttleSizeBatcher = new ThrottleSizeByteBatcher(size)
        IntStream.range(0, totalEvents).forEach({
            throttleSizeBatcher.append(getBytes(5000))
        })

        when:
        def batches = throttleSizeBatcher.getBatches()

        then:
        def flat = []
        batches.forEach({ it.forEach({ b -> flat.add(b)}) })
        !flat.any({
            it.length > size
        })

        where:
        size  | totalEvents    | expectedBatches
        10000 |       100      | (int)Math.ceil(10000/1000)   // 10 Batches
//        502   |        1000    | (int)Math.ceil(1000/502)   //  2 Batches
//        75    |         300    | (int)Math.ceil(300/75)     //  4 Batches
//        49    |         200    | (int)Math.ceil(200/49)     //  5 Batches
//        1     |           5    | (int)Math.ceil(5/1)        //  5 Batches
    }

    private byte[] getBytes(int size) {
        byte[] result = new byte[size]
        IntStream.range(0, size).forEach({ idx -> result[idx] = "b".getBytes()[0] })
        return result
    }
}
