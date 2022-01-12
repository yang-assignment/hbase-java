package org.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HBaseTest {
    //初始化Configuration对象
    private Configuration conf = null;
    //初始化连接
    private Connection conn = null;

    @Before
    public void init() throws Exception {
        //获取Configuration对象
        conf = HBaseConfiguration.create();
        //对于hbase的客户端来说，只需要知道hbase所经过的Zookeeper集群地址即可
        //因为hbase的客户端找hbase读写数据完全不用经过HMaster
        conf.set("hbase.zookeeper.quorum", "nosql01:2181,nosql02:2181,nosql03:2181");
        //获取连接
        conn = ConnectionFactory.createConnection(conf);
    }
    @Test
    public void createTable() throws Exception{
        //获取表管理器对象
        Admin admin = conn.getAdmin();
        //创建表的描述对象，并指定表名
        HTableDescriptor tableDescriptor =new HTableDescriptor(TableName
                .valueOf("t_phone_info".getBytes()));
        //构造第一个列族描述对象，并指定列族名
        HColumnDescriptor hcd1 = new HColumnDescriptor("base_info");
        //构造第二个列族描述对象，并指定列族名
        HColumnDescriptor hcd2 = new HColumnDescriptor("extra_info");
        //为该列族设定一个版本数量，最小为1，最大为3
        hcd2.setVersions(1,3);
        //将列族描述对象添加到表描述对象中
        tableDescriptor.addFamily(hcd1).addFamily(hcd2);
        //利用表管理器来创建表
        admin.createTable(tableDescriptor);
        //关闭
        admin.close();
        conn.close();
    }
    @Test
    public void testPut() throws Exception {
        //创建table对象，通过table对象来添加数据
        Table table = conn.getTable(TableName.valueOf("t_phone_info"));
        //创建一个集合，用于存放Put对象
        ArrayList<Put> puts = new ArrayList<Put>();
        //构建put对象（KV形式），并指定其行键
        Put put01 = new Put(Bytes.toBytes("p001"));
        put01.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("brand"),
                Bytes.toBytes("Apple"));
        put01.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("name"),
                Bytes.toBytes("iPhone 11 pro"));
        Put put02 = new Put("p002".getBytes());
        put02.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("name"),
                Bytes.toBytes("HUAWEI Mate 30 Pro"));
        put02.addColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("price"),
                Bytes.toBytes("5899"));
        //把所有的put对象添加到一个集合中
        puts.add(put01);
        puts.add(put02);
        //提交所有的插入数据的记录
        table.put(puts);
        //关闭
        table.close();
        conn.close();
    }
    @Test
    public void testGet() throws Exception {
        //获取一个table对象
        Table table = conn.getTable(TableName.valueOf("t_phone_info"));
        // 创建get查询参数对象，指定要获取的是哪一行
        Get get = new Get("p001".getBytes());
        //返回查询结果的数据
        Result result = table.get(get);
        //获取结果中的所有cell
        List<Cell> cells = result.listCells();
        //遍历所有的cell
        for(Cell cell:cells){
            //获取行键
            System.out.println("行:"+Bytes.toString(CellUtil.cloneRow(cell)));
            //得到列族
            System.out.println("列族:"+Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:"+Bytes.toString(CellUtil.cloneValue(cell)));
        }
        //关闭
        table.close();
        conn.close();
    }
    @Test
    public void testScan() throws Exception {
        //获取table对象
        Table table = conn.getTable(TableName.valueOf("t_phone_info"));
        //创建scan对象
        Scan scan = new Scan();
        //获取查询的数据
        ResultScanner scanner = table.getScanner(scan);
        //获取ResultScanner所有数据，返回迭代器
        Iterator<Result> iter = scanner.iterator();
        //遍历迭代器
        while (iter.hasNext()) {
            //获取当前每一行结果数据
            Result result = iter.next();
            //获取当前每一行中所有的cell对象
            List<Cell> cells = result.listCells();
            //迭代所有的cell
            for(Cell c:cells){
                //获取行键
                byte[] rowArray = c.getRowArray();
                //获取列族
                byte[] familyArray = c.getFamilyArray();
                //获取列族下的列名称
                byte[] qualifierArray = c.getQualifierArray();
                //列字段的值
                byte[] valueArray = c.getValueArray();
                //打印rowArray、familyArray、qualifierArray、valueArray
                System.out.println("行键:"+new String(rowArray,c.getRowOffset(), c.getRowLength()));
                System.out.print("列族:"+ new String(familyArray,c.getFamilyOffset(), c.getFamilyLength()));
                System.out.print(":"+"列:" + new String(qualifierArray, c.getQualifierOffset(),c.getQualifierLength()));
                System.out.println(" " +"值:" + new String(valueArray, c.getValueOffset(), c.getValueLength()));
            }
            System.out.println("-----------------------");
        }
        //关闭
        table.close();
        conn.close();
    }
    @Test
    public void testDel() throws Exception {
        //获取table对象
        Table table = conn.getTable(TableName.valueOf("t_phone_info"));
        //获取delete对象,需要一个rowkey
        Delete delete = new Delete("p001".getBytes());
        //在delete对象中指定要删除的列族-列名称
        delete.addColumn("base_info".getBytes(), "name".getBytes());
        //执行删除操作
        table.delete(delete);
        //关闭
        table.close();
        conn.close();
    }
    @Test
    public void testDrop() throws Exception {
        //获取一个表的管理器
        Admin admin = conn.getAdmin();
        //删除表时先需要disable，将表置为不可用，然后在delete
        admin.disableTable(TableName.valueOf("t_phone_info"));
        admin.deleteTable(TableName.valueOf("t_phone_info"));
        //关闭
        admin.close();
        conn.close();
    }





}
