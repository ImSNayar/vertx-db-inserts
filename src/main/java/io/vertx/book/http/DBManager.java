package io.vertx.book.http;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient;
import io.vertx.rxjava.ext.asyncsql.PostgreSQLClient;
import io.vertx.ext.web.Router;
import io.vertx.rxjava.ext.sql.SQLConnection;
import io.vertx.rxjava.ext.web.client.WebClient;
import rx.Single;
import scala.Int;

import javax.xml.transform.Result;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;

public class DBManager extends AbstractVerticle {
    SQLConnection connection = null;
    Random random = new Random();

    @Override
    public void start() {


        final AsyncSQLClient client = PostgreSQLClient.createShared(Vertx.vertx(), new JsonObject()
                .put("host", "postgre1.cmfjfmtp9cd0.us-west-1.rds.amazonaws.com")
                .put("username", "postgresql")
                .put("password", "postgresql")
                .put("database", "postgre1")
                .put("max_pool_size", 30));


        Router router = Router.router(vertx);
        router.get("/").handler(routingContext -> {
            JsonObject jsonObject = new JsonObject();
            jsonObject.put("GETcommand1", "/getCount");
            jsonObject.put("POSTcommand1", "/insert/:count");
            routingContext.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(jsonObject.encodePrettily());
        });

        router.get("/getCount").handler(routingContext -> {
            JsonObject jsonObject = new JsonObject();
            connection.query("Select count(*) from table1", result -> {
                if (result.succeeded()) {
                    jsonObject.put("Count", result.result().getResults().get(0).getLong(0));
                    routingContext.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(jsonObject.encodePrettily());
                } else {
                    jsonObject.put("Failure", result.cause().getMessage());
                    result.cause().printStackTrace();
                    System.err.println("Connection Failed!!! ");
                    routingContext.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(jsonObject.encodePrettily());
                }

            });

        });

        router.get("/insertCount/:count").handler(routingContext -> {
//            client.getConnection(sqlConnectionAsyncResult -> {
//                if (sqlConnectionAsyncResult.succeeded()) {
//                    SQLConnection conn = sqlConnectionAsyncResult.result();
//                    System.out.println("Attempting to autocommit");
//                    conn.setAutoCommit(true, result -> {
//                        System.out.println("Autocommit done");
//
//                        for (int i = 0; i < Integer.valueOf(routingContext.pathParam("count")); i++) {
//                            System.out.println("Executing Insert: " + i);
//                            conn.update("insert into table1 (id, searchkey) values ('" + UUID.randomUUID().toString()
//                                    + "', '" + random.nextInt() + "')", updateResultAsyncResult -> {
//                                if (updateResultAsyncResult.succeeded()) {
//                                    System.out.println(updateResultAsyncResult.result().getUpdated());
//                                } else {
//                                    routingContext.response().end("Failed" + updateResultAsyncResult.cause().getMessage());
//                                }
//                            });
//                        }
//                        routingContext.response().end("Attemped to insert many records");
//                    });
//
//                }
//                ;
//
//            });
            client.rxGetConnection().flatMap(sqlConnection -> {
                JsonObject jsonObject = new JsonObject();
                Single<UpdateResult> resultSetSingle = sqlConnection.rxUpdate("insert into table1 (id, searchkey) " +
                        "values ( '" + UUID.randomUUID().toString() + "', '" + Math.abs(new Random().nextInt()) + "')");

                for (int i = 0; i < Integer.valueOf(routingContext.pathParam("count")) - 1; i++) {
                    resultSetSingle =
                            resultSetSingle.flatMap(updateResult -> sqlConnection.rxUpdate("insert into table1" +
                                    " (id, searchkey) " +
                                    "values ( '" + UUID.randomUUID().toString().substring(0,30) + "', '" + Math.abs(new Random().nextInt()) + "')"));
                }

                Single<ResultSet> resultSetOutput =
                        resultSetSingle.flatMap(updateResult -> sqlConnection.rxQuery("select count(*) from table1"));
                return resultSetOutput.doAfterTerminate(sqlConnection::close);
            }).subscribe(resultSet -> {
                routingContext.response().end(String.valueOf(resultSet.getRows().get(0).getInteger("count")));
            }, throwable -> {
                routingContext.response().end(throwable.getMessage());

            });


        });
        vertx.createHttpServer().requestHandler(router::accept).listen(8082);
    }

}
