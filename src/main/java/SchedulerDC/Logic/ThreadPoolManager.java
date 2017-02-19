package SchedulerDC.Logic;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class ThreadPoolManager {

    private static final int SCHEDULER_THREADS = 10;

    private static ExecutorService _executorService;
    private static CompletionService _completionService;
    private static ScheduledExecutorService _scheduler;

    private static boolean _inited = false;

    private static class SingletonInstance {
        private static final ThreadPoolManager INSTANCE = new ThreadPoolManager();
    }

    private ThreadPoolManager() {
    }

    public static ThreadPoolManager getInstance() {
        return SingletonInstance.INSTANCE;
    }

    public void init(int threadsNumber) {
        if (_inited) {
            return;
        }

        _executorService = Executors.newWorkStealingPool(threadsNumber);
        _scheduler = Executors.newScheduledThreadPool(SCHEDULER_THREADS);
        _completionService = new ExecutorCompletionService<>(_executorService);

        _inited = true;
    }

    public void executeRunnable(Runnable runnable) {
        _executorService.execute(runnable);
    }

    public void executeFutureTask (Callable callable) {
        _completionService.submit(callable);
    }

    public Future getCompletionFutureTask() {
        Future future = null;
        try {
            future = _completionService.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return future;
    }

    public ScheduledFuture<?> scheduledTask(Runnable task, int intervalSec) {
        return  _scheduler.scheduleAtFixedRate(task, intervalSec, intervalSec, SECONDS);
    }

    public ScheduledFuture<Object> taskSchedulerService(LocalDateTime targetTime, Callable<Object> callableTask) {
        ZonedDateTime zonedTargetTime = targetTime.atZone(ZoneId.systemDefault());

        //time remaining, sec (for all time zones)
        long timeRemainingSec = zonedTargetTime.toEpochSecond() - ZonedDateTime.now().toEpochSecond();
        System.out.println(timeRemainingSec);

        //run expired tasks instantly
        return scheduledCallable(callableTask, timeRemainingSec > 0 ? timeRemainingSec : 0);
    }

    private ScheduledFuture<Object> scheduledCallable(Callable<Object> task, long intervalSec) {
        return  _scheduler.schedule(task, intervalSec, SECONDS);
    }

}