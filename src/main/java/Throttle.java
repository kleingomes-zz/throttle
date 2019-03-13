import java.io.Serializable;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Throttle implements Serializable  {

    private LinkedBlockingQueue<Runnable> linkedBlockingQueue;
    private AtomicInteger requestCount;
    public List<Long> timeSet = new ArrayList<>();
    private Timer timer = new Timer();
    private int requestsPerSecond;

    public Throttle(int requestsPerSecond) throws Exception {
        if (requestsPerSecond <= 0) {
            throw new Exception("request per second must be greater than 0");
        }
        this.requestsPerSecond = requestsPerSecond;
        this.linkedBlockingQueue = new LinkedBlockingQueue<>();
        this.requestCount = new AtomicInteger(0);
    }

    public void start() {
        timer.schedule(queueTask(), new Date(), 1000);
    }

    public void blockingUntilEmptyStart() throws InterruptedException {
        start();
        while(true) {
            if(linkedBlockingQueue.size() == 0) {
                break;
            } else {
                Thread.sleep(500);
            }
        }
    }

    public void stop() {
        timer.cancel();
    }

    public void enqueue(Runnable runnable) {
        linkedBlockingQueue.offer(runnable);
    }

    private TimerTask queueTask() {
        return new TimerTask() {
            @Override
            public void run() {
                requestCount.set(0);
                Runnable runnable = null;
                System.out.println("");
                try {
                    while(true) {
                        int requests = requestCount.incrementAndGet();
                        timeSet.add(System.currentTimeMillis()/1000);
                        //System.out.println(System.nanoTime()/1000000000 + " " + requests);
                        if (requests <= requestsPerSecond) {
                            runnable = linkedBlockingQueue.take();
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
