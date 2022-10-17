package com.hbase;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author hang.yuan
 * @Date 2022/10/17 10:04
 * @Description
 * @Ref
 */
public class HBaseDDL {

    // 声明一个连接的静态属性
    public static Connection connection = HBaseConnectionMultiThreaded.connection;

    /**
     * 创建命名空间
     *
     * @param namespace 命名空间名称
     */
    public static void createNameSpace(String namespace) throws IOException {
        // 1. 获取 admin
        Admin admin = connection.getAdmin();

        // 2.1 创建命令空间描述建造者 => 设计师
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
        // 2.2 给命令空间添加需求
        builder.addConfiguration("user", "yh");

        // 2.3 使用 builder 构造出对应的添加完参数的对象 完成创建
        // 创建命名空间出现的问题 都属于本方法自身的问题 不应该抛出
        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            System.out.println("CREATE NAMESPACE EXCEPTION");
            e.printStackTrace();
        }

        // 3. 关闭 admin
        admin.close();

    }

    /**
     * 判断表格是否存在
     *
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @return true 表存在
     */
    public static boolean isTableExists(String namespace, String tableName) throws IOException {

        // // 1. 获取 admin
        Admin admin = connection.getAdmin();

        // 2. 使用方法判断表格是否存在
        boolean flag = false;
        try {
            flag = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭 admin
        admin.close();

        // 4. 返回结果
        return flag;

    }

    /**
     * 创建表格
     *
     * @param namespace     命名空间名称
     * @param tableName     表格名称
     * @param columnFamilys 列族名称 可以有多个
     */
    public static void createTable(String namespace, String tableName, String... columnFamilys) throws IOException {
        // 判断是否有至少一个列族
        if (columnFamilys.length == 0) {
            System.out.println(" 创建表格至少有一个列簇 ");
            return;
        }

        // 判断表格是否存在
        if (isTableExists(namespace, tableName)) {
            System.out.println("Tabel Already Exists");
            return;
        }

        // 1.获取 admin
        Admin admin = connection.getAdmin();

        // 2. 调用方法创建表格
        // 2.1 创建表格描述的建造者
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));

        // 2.2 添加参数
        for (String columnFamily : columnFamilys) {
            // 2.3 创建列族描述的建造者
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
            // 2.4 对应当前的列族添加参数
            // 添加版本参数
            columnFamilyDescriptorBuilder.setMaxVersions(5);
            // 2.5 创建添加完参数的列族描述
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }
        // 2.6 创建对应的表格描述
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭 admin
        admin.close();

    }

    /**
     * 修改表格中一个列族的版本
     *
     * @param namespace    命名空间名称
     * @param tableName    表格名称
     * @param columnFamily 列族名称
     * @param version      版本
     */
    public static void modifyTable(String namespace, String tableName, String columnFamily, int version) throws IOException {
        // 判断表格是否存在
        if (!isTableExists(namespace, tableName)) {
            System.out.println("Tabel Not Exists");
            return;
        }

        // 1. 获取 admin
        Admin admin = connection.getAdmin();

        try {
            // 2. 调用方法修改表格
            // 2.1 获取之前的表格描述
            TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));

            // 2.2 创建一个表格描述建造者
            // 如果使用填写 tableName 的方法 相当于创建了一个新的表格描述建造者 没有之前的信息 、如果想要修改之前的信息 必须调用方法填写一个旧的表格描述
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);

            // 2.3 对应建造者进行表格数据的修改
            ColumnFamilyDescriptor columnFamily1 = descriptor.getColumnFamily(Bytes.toBytes(columnFamily));

            // 创建列族描述建造者、需要填写旧的列族描述
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily1);
            // 修改对应的版本
            columnFamilyDescriptorBuilder.setMaxVersions(version);
            // 此处修改的时候 如果填写的新创建 那么别的参数会初始化
            tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());
            admin.modifyTable(tableDescriptorBuilder.build());

        } catch (Exception e) {
            e.printStackTrace();
        }

        // 3. 关闭 admin
        admin.close();
    }

    /**
     * 删除表格
     *
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @return true 表示删除成功
     */
    public static boolean deleteTable(String namespace, String tableName) throws IOException {
        // 1. 判断表格是否存在
        if (!isTableExists(namespace, tableName)) {
            System.out.println("Table Not Exists");
            return false;
        }

        // 2. 获取 admin
        Admin admin = connection.getAdmin();

        // 3. 调用相关的方法删除表格
        try {
            admin.disableTable(TableName.valueOf(namespace, tableName));
            admin.deleteTable(TableName.valueOf(namespace, tableName));
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 4. 关闭 admin
        admin.close();

        return true;

    }


    public static void main(String[] args) throws IOException {

//        createNameSpace("test");

//        System.out.println("isTableExists: " + isTableExists("bigdata", "person"));

//        createTable("test", "person", "info", "msg");

//        modifyTable("test", "person", "info", 4);

//        System.out.println("deleteTable result: " + deleteTable("test", "person"));

        HBaseConnectionMultiThreaded.closeConnection();

    }

}
