package io.crate.jdbc.sample;

import com.google.gson.Gson;
import io.crate.shade.org.postgresql.jdbc.PgResultSetMetaData;
import io.crate.shade.org.postgresql.util.PGobject;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

class DataProvider {

    private static final String POST_TABLE = "guestbook.posts";
    private static final String COUNTRIES_TABLE = "guestbook.countries";
    private static final String IMAGE_TABLE = "guestbook_images";
    private static final String MY_APPROVAL = "my.t_approval_index";

    private final Gson gson = new Gson();

    private static Properties properties;
    private final String host;
    private final int httpPort;

    private CloseableHttpClient httpClient = HttpClients.createSystem();
    private Connection connection;

    DataProvider() throws SQLException {
        int psqlPort = Integer.parseInt(getProperty("crate.psql.port"));
        httpPort = Integer.parseInt(getProperty("crate.http.port"));
        host = getProperty("crate.host");
        try {
            connection = DriverManager.getConnection(
                    String.format(Locale.ENGLISH, "jdbc:crate://%s:%d/", host, psqlPort)
            );
        } catch (SQLException e) {
            throw new SQLException("Cannot connect to the database", e);
        }
    }

    static String getProperty(String name) {
        return properties().getProperty(name);
    }

    private static Properties properties() {
        if (properties == null) {
            try {
                properties = new Properties();
                properties.load(DataProvider.class.getResourceAsStream("/config.properties"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return properties;
    }

    List<Map<String, Object>> getPosts() throws SQLException {
        PreparedStatement statement = connection.prepareStatement(String.format(
                "SELECT p.*, c.name as country, c.geometry as area " +
                        "FROM %s AS p, %s AS c " +
                        "WHERE within(p.user['location'], c.geometry)" +
                        "ORDER BY p.created DESC", POST_TABLE, COUNTRIES_TABLE));
        ResultSet rs = statement.executeQuery();
        return resultSetToListOfMaps(rs);
    }

    private List<Map<String, Object>> resultSetToListOfMaps(ResultSet rs) throws SQLException {
        List<Map<String, Object>> posts = new ArrayList<>();
        while (rs.next()) {
            posts.add(resultSetToMap.apply(rs));
        }
        return posts;
    }

    private final CheckedFunction<ResultSet, Map<String, Object>> resultSetToMap = rs -> {
        PgResultSetMetaData metaData = (PgResultSetMetaData) rs.getMetaData();
        int resultSetSize = metaData.getColumnCount();

        Map<String, Object> map = new HashMap<>(resultSetSize);
        for (int i = 1; i <= resultSetSize; i++) {
            map.put(metaData.getColumnName(i), rs.getObject(i));
        }
        return map;
    };


    private final CheckedFunction<ResultSet, DataService.Approval> resultSetToApproval = rs -> {
        PgResultSetMetaData metaData = (PgResultSetMetaData) rs.getMetaData();
        int resultSetSize = metaData.getColumnCount();

        Map<String, Object> map = new HashMap<>(resultSetSize);
        for (int i = 1; i <= resultSetSize; i++) {
            map.put(metaData.getColumnName(i), rs.getObject(i));
        }

        return MapToBean.mapToBean(map, DataService.Approval.builder().build());
    };

    Map<String, Object> getPost(String id) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(String.format(
                "SELECT p.*, c.name as country, c.geometry as area " +
                        "FROM %s AS p, %s AS c " +
                        "WHERE within(p.user['location'], c.geometry) " +
                        "AND p.id = ?", POST_TABLE, COUNTRIES_TABLE));
        statement.setString(1, id);
        ResultSet results = statement.executeQuery();
        if (results.next()) {
            return resultSetToMap.apply(results);
        } else {
            return Collections.emptyMap();
        }
    }

    DataService.Approval getApproval(int id) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(String.format(
                "SELECT * FROM %s" +
                        " WHERE id = ?", MY_APPROVAL));
        statement.setInt(1, id);
        ResultSet results = statement.executeQuery();
        if (results.next()) {
            return resultSetToApproval.apply(results);
        } else {
            return DataService.Approval.builder().build();
        }
    }

    List<Map<String, Object>> insertPost(Map<String, Object> post) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(String.format(
                "INSERT INTO %s " +
                        "(id, user, text, image_ref, created, like_count) " +
                        "VALUES (?, ?, ?, ?, ?, ?)", POST_TABLE));

        String id = UUID.randomUUID().toString();
        statement.setString(1, id);

        // objects can be streamed as json strings,
        // https://crate.io/docs/reference/en/latest/protocols/postgres.html#jdbc
        PGobject userObject = new PGobject();
        userObject.setType("json");
        userObject.setValue(gson.toJson(post.get("user")));
        statement.setObject(2, userObject);

        statement.setString(3, (String) post.get("text"));
        statement.setString(4, (String) post.get("image_ref"));
        statement.setLong(5, System.currentTimeMillis());
        statement.setLong(6, 0);
        if (statement.executeUpdate() == 0) {
            return Collections.emptyList();
        }
        connection.createStatement().execute(String.format("REFRESH TABLE %s", POST_TABLE));
        return Collections.singletonList(getPost(id));
    }

    DataService.Approval insertApproval(DataService.Approval approval) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(String.format(
                "INSERT INTO " +
                        "%s " +
                        "(id, approval_id, created_at, updated_at, title, content, tenant_id, approval_number) " +
                        "VALUES (?, ?, ?, ?, ?, ?,?, ?)", MY_APPROVAL));


        statement.setInt(1, approval.getId());
        statement.setString(2, approval.getApproval_id());
        statement.setTimestamp(3, approval.getCreated_at());
        statement.setTimestamp(4, approval.getUpdated_at());
        statement.setString(5, approval.getTitle());
        statement.setString(6, approval.getContent());
        statement.setInt(7, approval.getTenant_id());
        statement.setString(8, approval.getApproval_number());

        if (statement.executeUpdate() == 0) {
            return DataService.Approval.builder().build();
        }

        connection.createStatement().execute(String.format("REFRESH TABLE %s", MY_APPROVAL));
        return getApproval(approval.getId());
    }


    Map<String, Object> updatePost(String id, String val) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(String.format(
                "UPDATE %s " +
                        "SET text = ? " +
                        "WHERE id = ?", POST_TABLE));
        statement.setString(1, val);
        statement.setString(2, id);
        if (statement.executeUpdate() == 0) {
            return Collections.emptyMap();
        }
        connection.createStatement().execute(String.format("REFRESH TABLE %s", POST_TABLE));
        return getPost(id);
    }

    Map<String, Object> incrementLike(String id) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(String.format(
                "UPDATE %s " +
                        "SET like_count = like_count + 1 " +
                        "WHERE id = ?", POST_TABLE));
        statement.setString(1, id);
        if (statement.executeUpdate() == 0) {
            return Collections.emptyMap();
        }
        connection.createStatement().execute(String.format("REFRESH TABLE %s", POST_TABLE));
        return getPost(id);
    }

    boolean deletePost(String id) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(String.format(
                "DELETE FROM %s " +
                        "WHERE id = ?", POST_TABLE));
        statement.setString(1, id);
        return statement.executeUpdate() == 1;
    }

    List<Map<String, Object>> getBlobs() throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery(String.format(
                "SELECT digest, last_modified " +
                        "FROM %s " +
                        "ORDER BY 2 DESC", String.format("blob.%s", IMAGE_TABLE)));
        return resultSetToListOfMaps(rs);
    }

    CloseableHttpResponse getBlob(String digest) throws IOException {
        HttpGet get = new HttpGet(blobUri(digest));
        return httpClient.execute(get);
    }

    Map<String, String> insertBlob(String digest, byte[] body) throws IOException {
        String uri = blobUri(digest);
        HttpPut put = new HttpPut(uri);
        if (body != null) {
            put.setEntity(new ByteArrayEntity(body));
        }
        CloseableHttpResponse response = httpClient.execute(put);
        return Collections.unmodifiableMap(new HashMap<String, String>() {{
            put("digest", digest);
            put("url", "/image/" + digest);
            put("status", String.valueOf(response.getStatusLine().getStatusCode()));
        }});
    }

    CloseableHttpResponse deleteBlob(String digest) throws IOException {
        HttpDelete delete = new HttpDelete(blobUri(digest));
        return httpClient.execute(delete);
    }

    boolean blobExists(String digest) throws IOException {
        HttpHead head = new HttpHead(blobUri(digest));
        CloseableHttpResponse response = httpClient.execute(head);
        return response.getStatusLine().getStatusCode() == 200;
    }

    private String blobUri(String digest) {
        return String.format(Locale.ENGLISH,
                "http://%s:%s/_blobs/%s", host, httpPort, blobResourceUri(IMAGE_TABLE, digest));
    }

    private String blobResourceUri(String index, String digest) {
        return String.format(Locale.ENGLISH, "%s/%s", index, digest);
    }

    List<Map<String, Object>> searchPosts(String query) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(String.format(
                "SELECT p.*, p._score as _score, c.name as country, c.geometry as area " +
                        "FROM %s AS p, %s AS c " +
                        "WHERE within(p.user['location'], c.geometry)" +
                        "AND match(text, ?) " +
                        "ORDER BY _score DESC", POST_TABLE, COUNTRIES_TABLE));
        statement.setString(1, query);
        ResultSet results = statement.executeQuery();
        return resultSetToListOfMaps(results);
    }

    @FunctionalInterface
    interface CheckedFunction<T, R> {
        R apply(T t) throws SQLException;
    }
}
