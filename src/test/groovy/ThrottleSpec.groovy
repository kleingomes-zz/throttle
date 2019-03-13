import org.apache.commons.math3.util.Precision
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder
import java.util.stream.Collectors
import java.util.stream.IntStream


class ThrottleSpec extends Specification {

    Throttle throttle

    @Unroll
    void 'it should send #totalEvents events at a rate of #eps events per second and complete in #expectedTotalTime seconds'() {
        given:
        List<Long> countList = []
        throttle = new Throttle(eps)
        IntStream.range(0, totalEvents).forEach({
            throttle.enqueue({
                countList.add(System.nanoTime()/1000000000 as Long)
            })
        })

        when:
        long startTime = System.currentTimeMillis()
        throttle.blockingUntilEmptyStart()
        long endTime = System.currentTimeMillis()
        long elapsedTime = endTime - startTime
        int upperLimit = (int) Precision.round(expectedTotalTime*1000, -3)
        int lowerLimit = upperLimit - 1000
        def countsPerSecond = countList.stream()
                .collect(Collectors.groupingBy({e -> e}, Collectors.counting()))

        then:
        !countsPerSecond.values().any({ it > eps})
        throttle.getQueueSize() == 0
        elapsedTime <= upperLimit && elapsedTime >= lowerLimit

        where:
        eps  | totalEvents    | expectedTotalTime
        1000 |       10000    | (int)Math.ceil(10000/1000) // 10 Seconds
        502  |        1000    | (int)Math.ceil(1000/502)   //  2 Seconds
        75   |         300    | (int)Math.ceil(300/75)     //  4 Seconds
        49   |         200    | (int)Math.ceil(200/49)     //  5 Seconds
        1    |           5    | (int)Math.ceil(5/1)        //  5 Seconds
    }

    void 'it should throw exception on 0 eps'() {
        when:
        throttle = new Throttle(0)

        then:
        thrown(Exception)
    }
}
