import com.sun.tools.javac.util.Pair;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.LongAdder;

public class ThrottleControl implements Serializable {

    private LinkedBlockingQueue<Pair<Integer, Runnable>> linkedBlockingQueue;
    private ConcurrentHashMap<Long, LongAdder> eventTrackerMap;
    private Timer timer;
    private int eventsPerSecond;
    private int maxFileSize;

    private BatchMode batchMode;

    public ThrottleControl(BatchMode batchMode) throws Exception {
        this.linkedBlockingQueue = new LinkedBlockingQueue<>();
        this.eventTrackerMap = new ConcurrentHashMap<>();
        this.timer = new Timer();
        this.batchMode = batchMode;
    }

    public void start() throws Exception {
        if (this.batchMode == BatchMode.BY_COUNT) {
            timer.schedule(queueCountEvent(), 0, 1000);
        } else if (this.batchMode == BatchMode.BY_FILE_SIZE) {
            timer.schedule(queueSizeEvent(), 0, 1000);
        } else {
            throw new Exception("Batch mode is not set");
        }
    }

    public void blockingStart(boolean keepAlive) throws InterruptedException {
        timer.schedule(queueCountEvent(), 0, 1000);
        while (linkedBlockingQueue.size() > 0 || keepAlive) {
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

    public void enqueueCount(Runnable runnable) throws Exception {
        if (batchMode == BatchMode.BY_COUNT) {
            linkedBlockingQueue.offer(Pair.of(null, runnable));
        }
        throw new Exception("Batch mode must be set to BY_COUNT");
    }

    public void enqueueSize(Runnable runnable, int eventSize) throws Exception {
        if (batchMode == BatchMode.BY_FILE_SIZE) {
            linkedBlockingQueue.offer(Pair.of(eventSize, runnable));
        }
        throw new Exception("Batch mode must be set to BY_SIZE");
    }

    private TimerTask queueSizeEvent() {
        return new TimerTask() {
            @Override
            public void run() {
                eventTrackerMap.clear();
                Runnable runnable;
                try {
                    while (true) {
                        long time = System.nanoTime() / 1000000000;
                        LongAdder longAdder = eventTrackerMap.computeIfAbsent(time, l -> new LongAdder());
                        if (longAdder.longValue() < maxFileSize) {
                            runnable = linkedBlockingQueue.take().snd;
                            int size = linkedBlockingQueue.take().fst;
                            longAdder.add(size);
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

    private TimerTask queueCountEvent() {
        return new TimerTask() {
            @Override
            public void run() {
                eventTrackerMap.clear();
                Runnable runnable;
                try {
                    while (true) {
                        long time = System.nanoTime() / 1000000000;
                        LongAdder longAdder = eventTrackerMap.computeIfAbsent(time, l -> new LongAdder());
                        if (longAdder.longValue() < eventsPerSecond) {
                            runnable = linkedBlockingQueue.take().snd;
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

    public int getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(int maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public void setEventsPerSecond(int eventsPerSecond) {
        this.eventsPerSecond = eventsPerSecond;
    }
}

