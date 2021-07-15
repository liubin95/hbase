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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private static Table tableRelationsFans;


    @BeforeAll
    static void beforeAll() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
        tableUser = connection.getTable(TableName.valueOf("weibo-user"));
        tableContent = connection.getTable(TableName.valueOf("weibo-content"));
        tableRelations = connection.getTable(TableName.valueOf("weibo-relations"));
        tableRelationsFans = connection.getTable(TableName.valueOf("weibo-relations-fans"));
    }

    @AfterAll
    static void afterAll() throws IOException {
        tableRelations.close();
        tableRelationsFans.close();
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

        // 关注表
        final TableName tableNameRelations = TableName.valueOf("weibo-relations");
        final ColumnFamilyDescriptor infoRelations = ColumnFamilyDescriptorBuilder
                .newBuilder("f".getBytes())
                .build();
        final TableDescriptor descriptorRelations = TableDescriptorBuilder
                .newBuilder(tableNameRelations)
                .setColumnFamilies(Collections.singletonList(infoRelations))
                .build();

        // 粉丝表
        final TableName tableNameRelationsFans = TableName.valueOf("weibo-relations-fans");
        final ColumnFamilyDescriptor infoRelationsFans = ColumnFamilyDescriptorBuilder
                .newBuilder("f".getBytes())
                .build();
        final TableDescriptor descriptorRelationsFans = TableDescriptorBuilder
                .newBuilder(tableNameRelationsFans)
                .setColumnFamilies(Collections.singletonList(infoRelationsFans))
                .build();

        admin.createTable(descriptor);
        admin.createTable(descriptorContent);
        admin.createTable(descriptorRelations);
        admin.createTable(descriptorRelationsFans);
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
     * <p>
     * 【张三 1】关注【李四 2】
     * 【张三 1】关注【王五 3】
     * </p>
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
        // value : followed userName
        final Result user = tableUser.get(get);
        final byte[] userName = CellUtil.cloneValue(user.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("uName")));
        // CQ : followed userid
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(id), userName);
        tableRelations.put(put);

        // 粉丝部分
        // followed + follower
        final byte[] rowKeyFans = Bytes.toBytes(id + uid);
        final Put putFans = new Put(rowKeyFans);
        final Get getFans = new Get(Bytes.toBytes(uid));
        // value : follower userName
        final Result userFans = tableUser.get(getFans);
        final byte[] userNameFans = CellUtil.cloneValue(userFans.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("uName")));
        // CQ : follower userid
        putFans.addColumn(Bytes.toBytes("f"), Bytes.toBytes(uid), userNameFans);
        tableRelationsFans.put(putFans);

    }

    /**
     * 查看关注人列表
     *
     * @throws IOException IOException
     */
    @Test
    void followedList() throws IOException {
        final String id = System.getProperty("id");
        final Set<String> followerList = getFollowedList(tableRelations, id);
        followerList.forEach(LOGGER::info);
    }

    /**
     * 查看粉丝列表
     *
     * @throws IOException IOException
     */
    @Test
    void fansList() throws IOException {
        final String id = System.getProperty("id");
        final Set<String> followerList = getFollowedList(tableRelationsFans, id);
        followerList.forEach(LOGGER::info);
    }

    /**
     * 查看关注人列表
     *
     * @param id    id
     * @param table tableRelations 即关注人列表；tableRelationsFans 即粉丝列表
     * @return [id : userName]
     * @throws IOException IOException
     */
    private static Set<String> getFollowedList(Table table, String id) throws IOException {
        final Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(id));
        scan.withStopRow(Bytes.toBytes(id + "|"));
        final ResultScanner scanner = table.getScanner(scan);
        final Set<String> strings = new HashSet<>();
        for (Result result : scanner) {
            final List<Cell> cells = result.listCells();
            cells.stream().map(cell -> {
                final String userId = Bytes.toString(CellUtil.cloneQualifier(cell));
                final String userName = Bytes.toString(CellUtil.cloneValue(cell));
                return userId + " : " + userName;
            }).forEach(strings::add);
        }
        return strings;
    }

    /**
     * 获取用户的微博
     *
     * @param string id : userName
     * @return [userName, content]
     * @throws IOException IOException
     */
    private static Map<String, String> getUserWeibo(String string) throws IOException {
        final String[] split = string.split(" : ");
        final String userId = split[0];
        final String userName = split[1];
        final Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(userId));
        scan.withStopRow(Bytes.toBytes(userId + "|"));
        final ResultScanner scanner = tableContent.getScanner(scan);
        final Map<String, String> map = new HashMap<>();
        for (Result result : scanner) {
            final Cell cell = result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("content"));
            final String content = Bytes.toString(CellUtil.cloneValue(cell));
            map.put(userName, content);
        }
        return map;
    }

    /**
     * 获取推送用户的内容
     * <p>
     * 即关注人的微博列表
     * </p>
     *
     * @throws IOException IOException
     */
    @Test
    void followerFeed() throws IOException {
        final String id = System.getProperty("id");
        final Set<String> strings = getFollowedList(tableRelations, id);
        for (String string : strings) {
            final Map<String, String> userWeibo = getUserWeibo(string);
            userWeibo.forEach((k, v) -> LOGGER.info("{} : {}", k, v));
        }
    }

    /**
     * uid取消关注id
     * <p>
     * 【张三 1】取消关注【李四 2】
     * </p>
     *
     * @throws IOException IOException
     */
    @Test
    void checkOff() throws IOException {
        // 被关注人
        final String id = System.getProperty("id");
        // 关注人
        final String uid = System.getProperty("uid");
        // 删除关注
        final byte[] rowKey1 = Bytes.toBytes(uid + id);
        final Delete delete1 = new Delete(rowKey1);
        tableRelations.delete(delete1);
        // 删除粉丝
        final byte[] rowKey2 = Bytes.toBytes(id + uid);
        final Delete delete2 = new Delete(rowKey2);
        tableRelationsFans.delete(delete2);
    }
}
