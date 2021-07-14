import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * TestApi.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-07-06 : base version.
 */
public class WeiboTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WeiboTest.class);

    private static Connection connection;

    private static Admin admin;

    private static Table tableUser;

    private static Table tableContent;

    private static Table tableRelations;


    @BeforeAll
    static void beforeAll() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
        tableUser = connection.getTable(TableName.valueOf("weibo-user"));
        tableContent = connection.getTable(TableName.valueOf("weibo-content"));
        tableRelations = connection.getTable(TableName.valueOf("weibo-relations"));
    }

    @AfterAll
    static void afterAll() throws IOException {
        tableUser.close();
        tableContent.close();
        admin.close();
        connection.close();
        LOGGER.info("关闭链接");
    }

    @Test
    public void creatTable() throws IOException {

        // 用户表
        final TableName tableName = TableName.valueOf("weibo-user");
        final ColumnFamilyDescriptor info = ColumnFamilyDescriptorBuilder
                .newBuilder("info".getBytes())
                .build();
        final TableDescriptor descriptor = TableDescriptorBuilder
                .newBuilder(tableName)
                .setColumnFamilies(Collections.singletonList(info))
                .build();

        // 内容表
        final TableName tableNameContent = TableName.valueOf("weibo-content");
        final TableDescriptor descriptorContent = TableDescriptorBuilder
                .newBuilder(tableNameContent)
                .setColumnFamilies(Collections.singletonList(info))
                .build();

        // 关系表
        final TableName tableNameRelations = TableName.valueOf("weibo-relations");
        final ColumnFamilyDescriptor infoRelations = ColumnFamilyDescriptorBuilder
                .newBuilder("f".getBytes())
                .build();
        final TableDescriptor descriptorRelations = TableDescriptorBuilder
                .newBuilder(tableNameRelations)
                .setColumnFamilies(Collections.singletonList(infoRelations))
                .build();

        admin.createTable(descriptor);
        admin.createTable(descriptorContent);
        admin.createTable(descriptorRelations);
    }

    @Test
    public void creatUser() throws IOException {
        final String uid = System.getProperty("uid");
        final String uName = System.getProperty("uName");

        final byte[] rowKey = Bytes.toBytes(uid);
        final Put put = new Put(rowKey);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("uName"), Bytes.toBytes(uName));
        tableUser.put(put);
    }

    @Test
    public void sendWeibo() throws IOException {
        final String uid = System.getProperty("uid");
        final String content = System.getProperty("content");
        final byte[] rowKey = Bytes.toBytes(uid + "_" + System.currentTimeMillis());
        final Put put = new Put(rowKey);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));
        tableContent.put(put);
    }

    @Test
    public void weiboDelete() throws IOException {
        final String id = System.getProperty("id");
        final byte[] rowKey = Bytes.toBytes(id);
        final Delete delete = new Delete(rowKey);
        tableContent.delete(delete);
    }

    /**
     * uid关注id
     *
     * @throws IOException IOException
     */
    @Test
    void follow() throws IOException {
        // 被关注人
        final String id = System.getProperty("id");
        // 关注人
        final String uid = System.getProperty("uid");
        // follower + followed
        final byte[] rowKey = Bytes.toBytes(uid + id);
        final Put put = new Put(rowKey);
        final Get get = new Get(Bytes.toBytes(id));
        // value:userName
        final Result user = tableUser.get(get);
        final byte[] userName = CellUtil.cloneValue(user.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("uName")));
        // CQ : followed userid
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(id), userName);
        tableRelations.put(put);
    }

    @Test
    void followerList() throws IOException {
        final String id = System.getProperty("id");
        final Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(id));
        scan.withStopRow(Bytes.toBytes(id + "|"));
        final ResultScanner scanner = tableRelations.getScanner(scan);
        for (Result result : scanner) {
            final List<Cell> cells = result.listCells();
            cells.stream().map(cell -> {
                final String userId = Bytes.toString(CellUtil.cloneQualifier(cell));
                final String userName = Bytes.toString(CellUtil.cloneValue(cell));
                return userId + " : " + userName;
            }).forEach(LOGGER::info);
        }
    }
}
