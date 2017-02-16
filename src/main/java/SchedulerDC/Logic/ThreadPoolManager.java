package SchedulerDC.Logic;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class ThreadPoolManager {

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
        _scheduler = Executors.newScheduledThreadPool(1);
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

    public ScheduledFuture<Object> scheduledCallable(Callable<Object> task, long intervalSec) {
        return  _scheduler.schedule(task, intervalSec, SECONDS);
    }

}