import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
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


    @BeforeAll
    static void beforeAll() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
        tableUser = connection.getTable(TableName.valueOf("weibo-user"));
        tableContent = connection.getTable(TableName.valueOf("weibo-content"));
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
        final NamespaceDescriptor weibo = NamespaceDescriptor.create("weibo")
                .build();
        admin.createNamespace(weibo);

        final TableName tableName = TableName.valueOf("weibo-user");
        final ColumnFamilyDescriptor info = ColumnFamilyDescriptorBuilder
                .newBuilder("info".getBytes())
                .build();
        final TableDescriptor descriptor = TableDescriptorBuilder
                .newBuilder(tableName)
                .setColumnFamilies(Collections.singletonList(info))
                .build();

        final TableName tableNameContent = TableName.valueOf("weibo-content");
        final ColumnFamilyDescriptor infoContent = ColumnFamilyDescriptorBuilder
                .newBuilder("info".getBytes())
                .build();
        final TableDescriptor descriptorContent = TableDescriptorBuilder
                .newBuilder(tableNameContent)
                .setColumnFamilies(Collections.singletonList(infoContent))
                .build();
        admin.createTable(descriptor, Bytes.toBytes("000|"), Bytes.toBytes("004|"), 5);
        admin.createTable(descriptorContent, Bytes.toBytes("000|"), Bytes.toBytes("004|"), 5);
    }

    @Test
    public void creatUser(Long uid) throws IOException {
        final byte[] rowKey = Bytes.toBytes("00" + uid % 5 + "_" + System.currentTimeMillis());
        final Put put = new Put(rowKey);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("attention"), Bytes.toBytes(""));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fans"), Bytes.toBytes(""));
        tableUser.put(put);
    }

    @Test
    public void sendWeibo(Long uid, String content) throws IOException {
        final byte[] rowKey = Bytes.toBytes("00" + uid % 5 + "_" + System.currentTimeMillis());
        final Put put = new Put(rowKey);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));
        tableContent.put(put);
    }

    @Test
    public void weiboDelete(String id) throws IOException {
        final byte[] rowKey = Bytes.toBytes(id);
        final Delete delete = new Delete(rowKey);
        tableContent.delete(delete);
    }
}
