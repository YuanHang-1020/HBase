package com.hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import javax.print.DocFlavor;
import java.io.IOException;

/**
 * @Author hang.yuan
 * @Date 2022/10/17 10:04
 * @Description
 * @Ref
 */
public class HBaseDML {

    // 声明一个连接的静态属性
    public static Connection connection = HBaseConnectionMultiThreaded.connection;

    /**
     * put 插入数据
     *
     * @param namespace    命名空间名称
     * @param tableName    表格名称
     * @param rowKey       主键
     * @param columnFamily 列族名称
     * @param columnName   列名
     * @param value        列值
     */
    public static void putCell(String namespace, String tableName, String rowKey, String columnFamily, String columnName, String value) throws IOException {

        // 1. 获取 table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 调用相关方法插入数据
        // 2.1 创建 put 对象
        Put put = new Put(Bytes.toBytes(rowKey));

        // 2.2. 给 put 对象添加数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));

        // 2.3 将对象写入对应的方法
        try {
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 3. 关闭 table
        table.close();

    }

    /**
     * 读取数据 读取对应的一行中的某一列
     *
     * @param namespace    命名空间名称
     * @param tableNmae    表格名称
     * @param rowKey       主键
     * @param columnFamily 列族名称
     * @param columnName   列名
     */
    public static void getCells(String namespace, String tableNmae, String rowKey, String columnFamily, String columnName) throws IOException {
        // 1. 获取 table
        Table table = connection.getTable(TableName.valueOf(namespace, tableNmae));
        // 2. 创建 get 对象
        Get get = new Get(Bytes.toBytes(rowKey));
        // 如果直接调用 get 方法读取数据 此时读一整行数据
        // 如果想读取某一列的数据 需要添加对应的参数
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        // 设置读取数据的版本
        get.readAllVersions();

        try {
            // 读取数据 得到 result 对象
            Result result = table.get(get);
            // 处理数据
            Cell[] cells = result.rawCells();
            // 测试方法: 直接把读取的数据打印到控制台
            // 如果是实际开发 需要再额外写方法 对应处理数据
            for (Cell cell : cells) {
                // cell 存储数据比较底层
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println(value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 关闭 table
        table.close();

    }

    /**
     * 扫描数据
     *
     * @param namespace 命名空间
     * @param tableName 表格名称
     * @param startRow  开始的 row 包含的
     * @param stopRow   结束的 row 不包含
     */
    public static void scanRows(String namespace, String tableName, String startRow, String stopRow) throws IOException {
        // 1. 获取 table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建 scan 对象
        Scan scan = new Scan();

        // 如果此时直接调用 会直接扫描整张表; 可以添加参数 来控制扫描的数据
        // 默认包含
        scan.withStartRow(Bytes.toBytes(startRow));
        // 默认不包含
        scan.withStopRow(Bytes.toBytes(stopRow));
        try {
            ResultScanner scanner = table.getScanner(scan);
            System.out.println("ROW" + "\t\t\t\t\t " + "COLUMN+CELL");
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.println(
                            new String(CellUtil.cloneRow(cell)) + "\t\t\t\t " + "column=" + new String(CellUtil.cloneFamily(cell)) + ":" +
                                    new String(CellUtil.cloneQualifier(cell)) + ", timestamp=" + cell.getTimestamp() + ", value=" + new String(CellUtil.cloneValue(cell))
                    );
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 3. 关闭 table
        table.close();

    }

    /**
     * 带过滤的扫描
     *
     * @param namespace    命名空间
     * @param tableName    表格名称
     * @param startRow     开始的 row 包含的
     * @param stopRow      结束的 row 不包含
     * @param columnFamily 列族名称
     * @param columnName   列名
     * @param value        value 值
     */
    public static void filterScan(String namespace, String tableName, String startRow, String stopRow, String columnFamily, String columnName, String value) throws IOException {
        // 1. 获取 table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建 scan 对象
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));

        // 可以添加多个过滤
        FilterList filterList = new FilterList();

        // 创建过滤器
        // (1) 结果只保留当前列的数据
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), CompareOperator.EQUAL, Bytes.toBytes(value));
        // (2) 结果保留整行数据
        // 结果同时会保留没有当前列的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), CompareOperator.EQUAL, Bytes.toBytes(value));

        filterList.addFilter(columnValueFilter);
        /** result:
         * ROW					 COLUMN+CELL
         * 2002				 column=info:age, timestamp=1665990556548, value=20
         */
//        filterList.addFilter(singleColumnValueFilter);
        /** result:
         * ROW					 COLUMN+CELL
         * 2002				 column=info:age, timestamp=1665990556548, value=20
         * 2002				 column=info:gender, timestamp=1665991749542, value=F
         */
        scan.setFilter(filterList);

        try {
            ResultScanner scanner = table.getScanner(scan);
            System.out.println("ROW" + "\t\t\t\t\t " + "COLUMN+CELL");
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.println(
                            new String(CellUtil.cloneRow(cell)) + "\t\t\t\t " + "column=" + new String(CellUtil.cloneFamily(cell)) + ":" +
                                    new String(CellUtil.cloneQualifier(cell)) + ", timestamp=" + cell.getTimestamp() + ", value=" + new String(CellUtil.cloneValue(cell))
                    );
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 3. 关闭 table
        table.close();

    }


    public static void deleteColumn(String namespace, String tableName, String rowKey, String columnFamily, String columnName) throws IOException {

        // 1.获取 table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2.创建 Delete 对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 3.添加删除信息
        // 3.1 删除单个版本
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        // 3.2 删除所有版本
//        delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        // 3.3 删除列族
//        delete.addFamily(Bytes.toBytes(columnFamily));

        // 4.删除数据
        table.delete(delete);

        // 5.关闭资源
        table.close();

    }


    public static void main(String[] args) throws IOException {

//        putCell("bigdata", "person", "2001", "info", "age", "10");
//        putCell("bigdata", "person", "2001", "info", "gender", "M");

//        getCells("bigdata", "person", "2001", "info", "age");

//        scanRows("bigdata", "person", "2001", "info");

//        filterScan("bigdata", "person", "2001", "2004", "info", "age", "20");

//        deleteColumn("bigdata", "person", "2001", "info", "age");


        HBaseConnectionMultiThreaded.closeConnection();

    }

}
