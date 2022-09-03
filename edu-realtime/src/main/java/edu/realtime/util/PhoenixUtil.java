package edu.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 统一进行执行sql异常的处理
 */
public class PhoenixUtil {
    public static void executeSql(String sql, Connection conn) {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(preparedStatement, conn);
        }
    }

    private static void close(PreparedStatement preparedStatement, Connection conn) {
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 封装为任何JDBC数据库的任何查询都可以使用该工具类的方法
     * id为主键
     * 最后的返回结果有可能是  单行单列，单行多列，单列多行，多行多列
     */
    public static <T> List<T> sqlQuery(Connection conn, String sql, Class<T> clz) {
        //1.创建返回结果的集合
        ArrayList<T> result = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //2.预编译sql
            ps = conn.prepareStatement(sql);
            //3.执行sql查询
            rs = ps.executeQuery();
            //获取结果集对应的元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            //4.遍历查询结果集，将每行数据封装为T对象并放入集合
            while (rs.next()) {
                //获取t对象，后面给t对象进行赋值
                T t = clz.newInstance();
                //todo 因为结果集的角标长度无法确定，只能通过元数据的个数进行循环
                for (int i = 1; i <= columnCount; i++) {
                    //获取对应的列名
                    String columnName = metaData.getColumnName(i);
                    //todo 获取对应的值，可以用下下标获取，也可以用列名获取
                    Object value = rs.getObject(columnName);
                    //TODO 由于命名规则不一样，所以需要转换格式，用工具类CaseFormat
                    //第一个是需要转换之前的数据类型，第二个to后面是转换之后的数据类型，最后是将谁传进去进行转换
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                    //todo 通过工具类将对象与属性名，属性值进行匹配
                    BeanUtils.setProperty(t, columnName, value);
                }
                result.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("读取维度表数据有误");
        } finally {
           /* if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }*/
            close(ps,conn);
        }
        //5.返回结果的集合
        return result;
    }

    public static void main(String[] args) throws Exception {
        DruidDataSource dataSource = DruidPhoenixDSUtil.getDataSource();
        DruidPooledConnection connection = dataSource.getConnection();

        List<JSONObject> result = sqlQuery(connection, "select * from GMALL2022_REALTIME.DIM_BASE_TRADEMARK",
                JSONObject.class);

        for (JSONObject jsonObject : result) {
            System.out.println(jsonObject);
        }
        connection.close();
        dataSource.close();
    }
}
