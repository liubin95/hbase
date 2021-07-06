import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
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
public class TestApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestApi.class);

    private static Connection connection;

    private static Admin admin;


    @BeforeAll
    static void beforeAll() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        LOGGER.info(config.toString());
        connection = ConnectionFactory.createConnection(config);
        LOGGER.info(connection.toString());
        admin = connection.getAdmin();
        LOGGER.info(admin.toString());
    }

    @AfterAll
    static void afterAll() throws IOException {
        connection.close();
        admin.close();
        LOGGER.info("关闭链接");
    }


    @Test
    public void tableExist() throws IOException {
        final TableName tableName = TableName.valueOf("liubin:student");
        LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>> "+admin.tableExists(tableName));
    }
}
