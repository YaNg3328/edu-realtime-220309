package edu.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import edu.realtime.common.EduConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) {
        JSONObject result = null;
        //todo 查hbase之前，先查redis
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String value = jedis.get(redisKey);
        if (value != null) {
            //todo redis中有数据，直接返回，并更新过期时间
            result = JSON.parseObject(value);
            jedis.expire(redisKey,60*60*24);
        } else {
            // todo redis中没数据，查hbase，并写入redis，设置过期时间，注意判断返回值的长度是否大于0
            List<JSONObject> dimInfos = getDimInfo(connection, tableName, new Tuple2<>("ID", id));
            if (dimInfos.size() > 0) {
                result = dimInfos.get(0);
                jedis.set(redisKey,result.toJSONString());
                jedis.expire(redisKey,60*60*24);
            }
        }

        if (jedis != null) {
            jedis.close();
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public static List<JSONObject> getDimInfo(Connection connection, String tableName, Tuple2<String, String>... tuple2s) {
        //拼接sql查询数据
        StringBuilder sql = new StringBuilder();
        //eg:select * from db.t where k1=v1 and k2=v2
        sql.append("select * from ")
                .append(EduConfig.HBASE_SCHEMA)
                .append(".")
                .append(tableName)
                .append(" where ");
        for (int i = 0; i < tuple2s.length; i++) {
            sql.append(tuple2s[i].f0)
                    .append(" = '")
                    .append(tuple2s[i].f1)
                    .append("'");
            if (i < tuple2s.length - 1) {
                sql.append(" and ");
            }
        }
        System.out.println(sql.toString());
        return PhoenixUtil.sqlQuery(connection, sql.toString(), JSONObject.class);
    }

    public static void deleteRedisCache(String tableName,String id){
        String redisKey = "DIM:" + tableName + ":" + id;
        Jedis jedis = JedisUtil.getJedis();
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        DruidDataSource dataSource = DruidPhoenixDSUtil.getDataSource();
        DruidPooledConnection connection = dataSource.getConnection();
        Jedis jedis = JedisUtil.getJedis();

        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_CATEGORY3", "1"));
        long end = System.currentTimeMillis();
        System.out.println(end-start); //288  257

        System.out.println(getDimInfo(connection, "DIM_BASE_CATEGORY2", "1"));
        long end2 = System.currentTimeMillis();
        System.out.println(end2-end); //10

        connection.close();
        dataSource.close();
    }
}
