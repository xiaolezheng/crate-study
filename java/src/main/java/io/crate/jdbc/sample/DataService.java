package io.crate.jdbc.sample;

import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by xiaolezheng on 17/6/29.
 */
@Slf4j
public class DataService {

    public static void main(String[] args) throws Exception {
        DataProvider provider = new DataProvider();
        for(int i=10; i<15;i++) {
            final Integer j = i;
            new Thread(()->{
                try {
                    List<Approval> result = Lists.newArrayList();
                    int start = j * 1000 + 1;
                    int end = (j + 1) * 1000;
                    getDataFromMysql(result, start, end);

                    for (Approval approval : result) {
                        Approval indexItem = provider.insertApproval(approval);

                        log.info("index: {}", indexItem);
                    }
                }catch (Exception e){
                    log.error("",e);
                }
            }).start();
        }

        TimeUnit.MINUTES.sleep(20);
    }

    public static void getDataFromMysql(List<Approval> result, int start, int end) throws Exception {
        Connection conn = null;
        String url = "jdbc:mysql://10.10.30.40:3307/oa_security?characterEncoding=UTF8";
        try {
            Class.forName("com.mysql.jdbc.Driver");// 动态加载mysql驱动
            log.info("成功加载MySQL驱动程序");
            // 一个Connection代表一个数据库连接
            conn = DriverManager.getConnection(url, "oa_security", "PZTH%WGm0vPKUNZ)abce6&sHYU9iT+D?");

            // Statement里面带有很多方法，比如executeUpdate可以实现插入，更新和删除等
            Statement stmt = conn.createStatement();
            String sql = String.format("select * from t_approval_index order by id limit %s,%s", start, end);
            ResultSet resultSet = stmt.executeQuery(sql);


            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String approval_id = resultSet.getString("approval_id");
                Timestamp created_at = resultSet.getTimestamp("created_at");
                Timestamp updated_at = resultSet.getTimestamp("updated_at");
                String title = resultSet.getString("title");
                String content = resultSet.getString("content");
                int tenant_id = resultSet.getInt("tenant_id");
                String approval_number = resultSet.getString("approval_number");

                Approval approval = Approval.builder().id(id).approval_id(approval_id).created_at(created_at)
                        .updated_at(updated_at).title(title).content(content).tenant_id(tenant_id).approval_number(approval_number).build();

                log.info("{}", approval);

                result.add(approval);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }
    }


    @Data
    @Builder
    static class Approval {
        private int id;
        private int tenant_id;
        private Timestamp created_at;
        private Timestamp updated_at;
        private String approval_id;
        private String title;
        private String content;
        private String approval_number;
    }
}
