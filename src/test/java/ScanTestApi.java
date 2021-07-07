import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
public class ScanTestApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanTestApi.class);

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
    public void TableScan() throws IOException {
        final Scan scan = new Scan();
        final ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            StringBuilder stringBuilder = new StringBuilder();
            for (Cell cell : result.rawCells()) {
                final String r = Bytes.toString(CellUtil.cloneRow(cell));
                final String f = Bytes.toString(CellUtil.cloneFamily(cell));
                final String q = Bytes.toString(CellUtil.cloneQualifier(cell));
                final String v = Bytes.toString(CellUtil.cloneValue(cell));
                stringBuilder.append(String.format("%s : %s : %s : %s", r, f, q, v));
                stringBuilder.append("\t");
            }
            LOGGER.info(stringBuilder.toString());
        }
    }

}
