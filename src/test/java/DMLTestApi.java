import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * TestApi.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-07-06 : base version.
 */
public class DMLTestApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(DMLTestApi.class);

    private static Connection connection;

    private static Table table;


    @BeforeAll
    static void beforeAll() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        connection = ConnectionFactory.createConnection(config);
        final TableName tableName = TableName.valueOf("liubin:stu");
        table = connection.getTable(tableName);
    }

    @AfterAll
    static void afterAll() throws IOException {
        connection.close();
        table.close();
        LOGGER.info("关闭链接");
    }


    @Test
    public void dataPut() throws IOException {
        // 创建put
        final byte[] rowKey = Bytes.toBytes(System.currentTimeMillis());
        // 列族
        Put put = new Put(rowKey);
        // 列族,列名,值
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        table.put(put);
    }
}
