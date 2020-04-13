package com.qiao.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DDLTest {

    Configuration configuration = null;
    static TableName tableName = null;


    @Before
    public void before() {
        configuration = HBaseConfiguration.create();
        tableName = TableName.valueOf("qiaochunyu:user_0415");
    }

    /**
     * 新建table
     */
    @Test
    public void createTable() {
        try (
                Connection connection = ConnectionFactory.createConnection(configuration);
                Admin admin = connection.getAdmin();
        ) {
            if (admin.tableExists(tableName)) {
                System.out.println("table " + tableName.toString() + " is exists!");
                return;
            }
            HTableDescriptor decs = new HTableDescriptor(tableName);

            HColumnDescriptor cf = new HColumnDescriptor(Bytes.toBytes("cf"));
            decs.addFamily(cf);

            admin.createTable(decs);

            System.out.println("create table seccesssed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除table
     */
//    @Test
    public void deleteTable() {
        try (
                Connection connection = ConnectionFactory.createConnection(configuration);
                Admin admin = connection.getAdmin()
        ) {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("delete table seccesssed");
                return;
            } else {
                System.out.println("table not exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 增加列族
     */
//    @Test
    public void alterAddTable() {
        try (
                Connection connection = ConnectionFactory.createConnection(configuration);
                Admin admin = connection.getAdmin()
        ) {
            if (admin.tableExists(tableName)) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes("c_d"));
                admin.addColumn(tableName, hColumnDescriptor);
                System.out.println("alter table seccesssed");
                return;
            } else {
                System.out.println("table not exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除列族
     */
//    @Test
    public void alterDeleteTable() {
        try (
                Connection connection = ConnectionFactory.createConnection(configuration);
                Admin admin = connection.getAdmin()
        ) {
            if (admin.tableExists(tableName)) {
                admin.deleteColumn(tableName, Bytes.toBytes("c_d"));
                System.out.println("alter table seccesssed");
                return;
            } else {
                System.out.println("table not exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 增加数据
     */
    public void putData() {
        try (
                Connection connection = ConnectionFactory.createConnection(configuration);
                HTable table = (HTable) connection.getTable(tableName);
                Admin admin = connection.getAdmin()
        ) {
            if (admin.tableExists(tableName)) {
                Put putApi01 = new Put(Bytes.toBytes("Api01"));
                Put putApi02 = new Put(Bytes.toBytes("Api02"));
                putApi01.addColumn(getByte("c_a"), getByte("name"), getByte("qiaoTest"));
                putApi02.addColumn(getByte("c_a"), getByte("name"), getByte("yangTest"));
                List list = new ArrayList();
                list.add(putApi01);
                list.add(putApi02);
                table.put(list);
                System.out.println("put table seccesssed");
                return;
            } else {
                System.out.println("table not exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取一条数据
     */
    public void getData() {
        try (
                Connection connection = ConnectionFactory.createConnection(configuration);
                HTable table = (HTable) connection.getTable(tableName);
                Admin admin = connection.getAdmin()
        ) {
            if (admin.tableExists(tableName)) {
                Get get = new Get(getByte("Api01"));
                get.addColumn(getByte("c_a"), getByte("name"));
                get.setTimeStamp(1554994937878L);
                Result result = table.get(get);

                printRowData(result);
                System.out.println("get table seccesssed");
                return;
            } else {
                System.out.println("table not exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询全表（加查询条件）
     */
    @Test
    public void scanTable() {
        try (
                Connection connection = ConnectionFactory.createConnection(configuration);
                HTable table = (HTable) connection.getTable(tableName);
                Admin admin = connection.getAdmin()
        ) {
            if (admin.tableExists(tableName)) {
                Scan scan = new Scan();

                // 设置查询条件 包含start  不包含stop
//                scan.setStartRow(getByte("id02"));
//			    scan.setStopRow(getByte("id04"));

                // 设置查询数据版本数量
                scan.setMaxVersions(1);
                ResultScanner scanner = table.getScanner(scan);
                for (Result r :
                        scanner) {
                    printRowData(r);
                }
                System.out.println("scan table seccesssed");
                return;
            } else {
                System.out.println("table not exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除行数据
     */
    @Test
    public void deleteRow() {
        try (
                Connection connection = ConnectionFactory.createConnection(configuration);
                HTable table = (HTable) connection.getTable(tableName);
                Admin admin = connection.getAdmin()
        ) {
            if (admin.tableExists(tableName)) {
                Delete delete = new Delete(getByte("id03"));
                table.delete(delete);
                System.out.println("delete row seccesssed");
                return;
            } else {
                System.out.println("table not exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除某个列数据
     */
    @Test
    public void deleteColumn() {
        try (
                Connection connection = ConnectionFactory.createConnection(configuration);
                HTable table = (HTable) connection.getTable(tableName);
                Admin admin = connection.getAdmin()
        ) {
            if (admin.tableExists(tableName)) {
                Delete delete = new Delete(getByte("id02"));
                delete.addColumn(getByte("c_a"), getByte("name"));
                table.delete(delete);
                System.out.println("delete Column seccesssed");
                return;
            } else {
                System.out.println("table not exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用过滤器查询全表
     */
    @Test
    public void scanTableByCompar() {
        try (
                Connection connection = ConnectionFactory.createConnection(configuration);
                HTable table = (HTable) connection.getTable(tableName);
                Admin admin = connection.getAdmin()
        ) {
            if (admin.tableExists(tableName)) {
                Scan scan = new Scan();
                SingleColumnValueFilter filter = null;
//                filter = getFileterByBinaryCompartator();
//                filter = getFileterByRegexStringComparator();
                filter = getFileterBySubstringComparator();
                scan.setFilter(filter);
                scan.setMaxVersions(1);
                ResultScanner scanner = table.getScanner(scan);
                for (Result r :
                        scanner) {
                    printRowData(r);
                }
                System.out.println("scan table seccesssed");
            } else {
                System.out.println("table not exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用二进制比较
     */
    public SingleColumnValueFilter getFileterByBinaryCompartator() {
        BinaryComparator comparator = new BinaryComparator(getByte("qiaochunyu"));
        SingleColumnValueFilter filter = new SingleColumnValueFilter(getByte("c_a"), getByte("name"), CompareOp.EQUAL, comparator);
        filter.setFilterIfMissing(true);
        return filter;
    }


    /**
     * 使用正则比较
     */
    public SingleColumnValueFilter getFileterByRegexStringComparator() {
        RegexStringComparator comparator = new RegexStringComparator("^yang");
        SingleColumnValueFilter filter = new SingleColumnValueFilter(getByte("c_a"), getByte("name"), CompareOp.EQUAL, comparator);
        return filter;
    }

    /**
     * 字符串包含
     *
     * @return
     */
    public SingleColumnValueFilter getFileterBySubstringComparator() {
        SubstringComparator comparator = new SubstringComparator("boy");
        SingleColumnValueFilter filter = new SingleColumnValueFilter(getByte("c_a"), getByte("sex"), CompareOp.EQUAL, comparator);
        return filter;
    }

    private static void printRowData(Result result) {
        Cell[] cells = result.rawCells();
        StringBuffer stringBuffer = new StringBuffer();
        for (Cell cell :
                cells) {
            String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            String colum = Bytes.toString(CellUtil.cloneQualifier(cell));
            long times = cell.getTimestamp();
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            stringBuffer.append(rowkey).append("\tcolumn=").append(cf).append(":")
                    .append(colum).append("\ttimestmap=").append(times)
                    .append("\tvalue=").append(value).append("\n");
            System.out.println(stringBuffer.toString());
        }
    }

    public static byte[] getByte(String str) {
        return Bytes.toBytes(str);
    }

}
