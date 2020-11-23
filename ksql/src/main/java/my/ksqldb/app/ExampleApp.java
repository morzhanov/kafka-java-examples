package my.ksqldb.app;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;

public class ExampleApp {

  public static String KSQLDB_SERVER_HOST = "localhost";
  public static int KSQLDB_SERVER_HOST_PORT = 8088;

  public static void main(String[] args) {
    ClientOptions options = ClientOptions.create().setHost(KSQLDB_SERVER_HOST).setPort(KSQLDB_SERVER_HOST_PORT);
    Client client = Client.create(options);

    String sql = "CREATE STREAM GITHUBEVENTS "
        + "(id VARCHAR, type VARCHAR, actorId VARCHAR, actorUrl VARCHAR, repoId VARCHAR, repoUrl VARCHAR, createdAt VARCHAR) "
        + "WITH (KAFKA_TOPIC='GithubEvents', VALUE_FORMAT='json');";
    try {
      client.executeStatement(sql).get();
    } catch (Exception e) {
      System.out.println("GITHUBEVENTS stream already exists");
    }

    sql = "CREATE STREAM GITHUBPUSHEVENTS " + "WITH (VALUE_FORMAT='json') " + "AS "
        + "SELECT id, actorId, actorUrl, repoId, repoUrl, createdAt " + "FROM GITHUBEVENTS "
        + "WHERE type = 'PushEvent' " + "EMIT CHANGES;";
    try {
      client.executeStatement(sql).get();
    } catch (Exception e) {
      System.out.println("GITHUBPUSHEVENTS stream already exists");
    }

    sql = "CREATE TABLE GITHUBEVENTAGGREGATES " + "WITH (VALUE_FORMAT='json') " + "AS " + "SELECT id, COUNT(*) "
        + "FROM GITHUBPUSHEVENTS " + "WINDOW TUMBLING (SIZE 1 MINUTE) " + "GROUP BY id " + "EMIT CHANGES;";
    try {
      client.executeStatement(sql).get();
    } catch (Exception e) {
      System.out.println(e);
      System.out.println("GITHUBPUSHEVENTS stream already exists");
    }

    client.close();
  }
}
