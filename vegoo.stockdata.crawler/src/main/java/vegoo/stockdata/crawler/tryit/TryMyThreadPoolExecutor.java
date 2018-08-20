package vegoo.stockdata.crawler.tryit;

import java.util.concurrent.TimeUnit;

import vegoo.commons.MyThreadPoolExecutor;


public class TryMyThreadPoolExecutor {

    static MyThreadPoolExecutor mutilSecPool = new MyThreadPoolExecutor(0, 5, 2, TimeUnit.SECONDS);
    
    static void print(String s){
        for(int i = 0; i < 2; ++i){
            System.out.println(Thread.currentThread().getName()+" /"+mutilSecPool.getPoolSize()+"  当前运行任务："+s);
            try {
               Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
 
    public static void main(String[] args){
        mutilSecPool.submit(()->print("1"));
        mutilSecPool.submit(()->print("2"));
        mutilSecPool.submit(()->print("3"));
        mutilSecPool.submit(()->print("4"));
        mutilSecPool.submit(()->print("5"));
        mutilSecPool.submit(()->print("6"));
        mutilSecPool.submit(()->print("7"));
        mutilSecPool.submit(()->print("8"));
        mutilSecPool.submit(()->print("9"));
        mutilSecPool.submit(()->print("10"));

        System.out.println("线程池线程数: " + mutilSecPool.getPoolSize());
        try {
            Thread.sleep(10000);
            System.out.println("线程池线程数: " + mutilSecPool.getPoolSize());
        }catch (Exception e){
            e.printStackTrace();
        }
        
        mutilSecPool.submit(()->print("11"));
        mutilSecPool.submit(()->print("12"));
        mutilSecPool.submit(()->print("13"));
        mutilSecPool.submit(()->print("14"));
        mutilSecPool.submit(()->print("15"));
        mutilSecPool.submit(()->print("16"));
        mutilSecPool.submit(()->print("17"));
        mutilSecPool.submit(()->print("18"));
        mutilSecPool.submit(()->print("19"));
        mutilSecPool.submit(()->print("20"));

        System.out.println("线程池线程数: " + mutilSecPool.getPoolSize());
        try {
            Thread.sleep(10000);
            System.out.println("线程池线程数: " + mutilSecPool.getPoolSize());
        }catch (Exception e){
            e.printStackTrace();
        }
         
        
        mutilSecPool.shutdown();
    }

}
