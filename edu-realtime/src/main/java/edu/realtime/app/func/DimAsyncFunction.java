package edu.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import edu.realtime.util.DimUtil;
import edu.realtime.util.DruidPhoenixDSUtil;
import edu.realtime.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {
    private String tableName = null;
    private DruidDataSource dataSource = null;
    ThreadPoolExecutor poolExecutor = null;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidPhoenixDSUtil.getDataSource();
        poolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        poolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    //获取连接
                    DruidPooledConnection connection = dataSource.getConnection();

                    //todo 需要获取表名，主键 ， 以及添加匹配到的维度信息
                    //1.表名通过构造器传参获取
                    //2.主键通过抽象方法获取
                    String id = getKey(input);

                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                    if (dimInfo != null) {
                        //3.将查询到的dimInfo添加到input中
                        join(input,dimInfo);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.out.println("关联维度表出错");
                }

                resultFuture.complete(Collections.singleton(input));
            }
        });
    }


    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        resultFuture.complete(Collections.singleton(input));
    }
}
