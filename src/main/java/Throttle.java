import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.LongAdder;

public class Throttle implements Serializable  {

    private LinkedBlockingQueue<Runnable> linkedBlockingQueue;
    private ConcurrentHashMap<Long, LongAdder> eventTrackerMap;
    private Timer timer;
    private int eventsPerSecond;

    public Throttle(int eventsPerSecond) throws Exception {
        if (eventsPerSecond <= 0) {
            throw new Exception("events per second must be greater than 0");
        }
        this.linkedBlockingQueue = new LinkedBlockingQueue<>();
        this.eventTrackerMap = new ConcurrentHashMap<>();
        this.timer = new Timer();
        this.eventsPerSecond = eventsPerSecond;
    }

    public void start() {
        timer.schedule(queueEvent(), 0,1000);
    }

    public void blockingUntilEmptyStart() throws InterruptedException {
        start();
        while (linkedBlockingQueue.size() > 0) {
            Thread.sleep(500);
        }
        stop();
    }

    public void stop() {
        timer.cancel();
        linkedBlockingQueue.clear();
    }

    public int getQueueSize() {
        return linkedBlockingQueue.size();
    }

    public void enqueue(List<Runnable> runnableList) {
        runnableList.forEach(this::enqueue);
    }

    public void enqueue(Runnable runnable) {
        linkedBlockingQueue.offer(runnable);
    }

    private TimerTask queueEvent() {
        return new TimerTask() {
            @Override
            public void run() {
                eventTrackerMap.clear();
                Runnable runnable;
                try {
                    while(true) {
                        long time = System.nanoTime() / 1000000000;
                        LongAdder longAdder = eventTrackerMap.computeIfAbsent(time, l -> new LongAdder());
                         if (longAdder.longValue() < eventsPerSecond) {
                            runnable = linkedBlockingQueue.take();
                            longAdder.increment();
                            runnable.run();
                        } else {
                            break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
    }
}
