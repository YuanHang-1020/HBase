package com.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @Author hang.yuan
 * @Date 2022/10/17 9:44
 * @Description 单线程创建连接
 * @Ref
 */
public class HBaseConnectionSingleThreaded {

    public static void main(String[] args) throws IOException {

        // 1. 创建配置对象
        Configuration conf = new Configuration();

        // 2. 添加配置参数
        conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");

        // 3. 创建 hbase 的连接
        // 默认使用同步连接
        Connection connection = ConnectionFactory.createConnection(conf);

        System.out.println(connection);

        connection.close();

    }

}
