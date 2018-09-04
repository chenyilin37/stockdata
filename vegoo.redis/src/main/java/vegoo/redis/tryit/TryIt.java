package vegoo.redis.tryit;

import redis.clients.jedis.Jedis;

public class TryIt {
	public static void main(String[] args) {

	    Jedis jedis = new Jedis("192.168.1.81", 6379);
	    jedis.auth("foobared");
	    System.out.println(jedis.ping());
	}

}
