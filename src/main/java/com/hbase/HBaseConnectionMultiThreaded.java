package com.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @Author hang.yuan
 * @Date 2022/10/17 9:44
 * @Description 多线程创建连接
 * @Ref
 */
public class HBaseConnectionMultiThreaded {

    // 设置静态属性 hbase 连接
    public static Connection connection = null;

    static {
        try {
            // 默认读取配置文件
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            System.out.println("GET CONNECTION FALIUED");
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭hbase连接
     * @throws IOException
     */
    public static void closeConnection() throws IOException {
        if(connection != null){
            connection.close();
        }
    }




    public static void main(String[] args) throws IOException {

        System.out.println(HBaseConnectionMultiThreaded.connection);

        HBaseConnectionMultiThreaded.closeConnection();

    }

}
