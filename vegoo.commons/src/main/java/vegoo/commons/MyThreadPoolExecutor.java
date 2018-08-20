package vegoo.commons;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
 
public class MyThreadPoolExecutor extends java.util.concurrent.ThreadPoolExecutor {
    private final AtomicInteger submittedTaskCount = new AtomicInteger(0);
 
    public MyThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit) {
    	this(corePoolSize, maximumPoolSize, keepAliveTime, unit, new TaskQueue());
    }
    
    private MyThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                TaskQueue workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        workQueue.setExecutor(this);
        prestartAllCoreThreads();
    }

    //执行完成后计数器减1
    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        submittedTaskCount.decrementAndGet();
    }
 
    public int getSubmittedTaskCount() {
        return submittedTaskCount.get();
    }
 
    @Override
    public void execute(Runnable command) {
        submittedTaskCount.incrementAndGet();
        try {
            super.execute(command);
        } catch (RejectedExecutionException rx) {
            if(super.getQueue() instanceof TaskQueue) {
                final TaskQueue queue = (TaskQueue) (super.getQueue());
                try {
                    if (!queue.force(command)) {  // 无限队列, 理论上不会失败;
                        submittedTaskCount.decrementAndGet();
                        throw new RejectedExecutionException("Queue capacity is full.");
                    }
                } catch(Exception x){
                    submittedTaskCount.decrementAndGet();
                    throw new RejectedExecutionException(x);
                }
            } else {
                submittedTaskCount.decrementAndGet();
                throw rx;
            }
        }
    }
    
    private static class TaskQueue extends LinkedBlockingQueue<Runnable>{
		private static final long serialVersionUID = 1L;
		private MyThreadPoolExecutor executor;
     
        public TaskQueue(){
            super();
            
        }
        public void setExecutor(MyThreadPoolExecutor executor) {
            this.executor = executor;
        }
        
        public boolean force(Runnable o) {
            if (executor.isShutdown()) {
                throw new RejectedExecutionException("Executor not running, can't force a command into the queue");
            }
            return super.offer(o); //forces the item onto the queue, to be used if the task is rejected
        }
     
        @Override
        public boolean offer(Runnable o) {
            int currentPoolThreadSize = executor.getPoolSize();
            // 线程数达到最大，添加到队列
            if(currentPoolThreadSize >= executor.getMaximumPoolSize()) {
                return super.offer(o);
            }
            // 有空闲线程，直接添加到队列
            if(executor.getSubmittedTaskCount() < currentPoolThreadSize) {
                return super.offer(o);
            }
            // 当前线程池数还不是最大，创建线程
            if(currentPoolThreadSize < executor.getMaximumPoolSize()){
                return false;
            }
     
            return super.offer(o);
        }
    }
    
    
}
