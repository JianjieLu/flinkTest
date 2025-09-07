package whu.edu.moniData.shenZhou;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolUtil {

    private static volatile JedisPool jedisPool = null;

    JedisPoolUtil() {}

    public static JedisPool getJedisPoolInstance(String host, int port) {

        if (null == jedisPool) {
            synchronized (JedisPoolUtil.class) {
                if (null == jedisPool) {
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    poolConfig.setMaxTotal(200); // 最大连接数
                    poolConfig.setMaxIdle(32); // 最大空闲连接数
                    poolConfig.setMinIdle(10); // 最小空闲连接数
                    poolConfig.setMaxWaitMillis(100 * 1000);
                    poolConfig.setBlockWhenExhausted(true);
                    poolConfig.setTestOnBorrow(true);  //ping PONG

                    // 这里超时时间设置1分钟会不会有点长
                    jedisPool = new JedisPool(poolConfig, host, port, 60000, "whdx123cgz666");
                }
            }
        }
        return jedisPool;
    }

    // 资源释放回收
    public static void release(Jedis jedis) {
        if(null != jedis) {
            jedis.close();
        }
    }

}
