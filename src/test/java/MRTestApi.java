import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * TestApi.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-07-06 : base version.
 */
public class MRTestApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(MRTestApi.class);

    private static Connection connection;

    private static Admin admin;


    @BeforeAll
    static void beforeAll() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
    }

    @AfterAll
    static void afterAll() throws IOException {
        connection.close();
        admin.close();
        LOGGER.info("关闭链接");
    }

    @Test
    public void createTableWithRegions() throws IOException {
        final TableName tableName = TableName.valueOf("stu2");
        final ColumnFamilyDescriptor info = ColumnFamilyDescriptorBuilder
                .newBuilder("info".getBytes())
                .build();
        final ColumnFamilyDescriptor exam = ColumnFamilyDescriptorBuilder
                .newBuilder("exam".getBytes())
                .build();
        final TableDescriptor descriptor = TableDescriptorBuilder
                .newBuilder(tableName)
                .setColumnFamilies(Arrays.asList(info, exam))
                .build();
        // 预设10个分区，会把【0-9】平均分成10个
        admin.createTable(descriptor, Bytes.toBytes("000|"), Bytes.toBytes("298|"), 10);
    }

}
