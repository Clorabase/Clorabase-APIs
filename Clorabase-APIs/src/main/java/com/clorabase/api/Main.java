package com.clorabase.api;


import com.clorabase.api.chat.ChatServer;
import com.clorabase.api.chat.ChatWebSocketHandler;
import com.clorabase.api.database.clorastore.ClorastoreDatabase;
import com.clorabase.api.database.clorem.DatabaseHandler;
import com.clorabase.api.database.clorem.DatabaseInstance;
import com.clorabase.api.datastore.ClorabaseDatastore;

import spark.Spark;

public class Main {
    public static void main(String[] args) {
        String port = System.getenv("PORT");
        Spark.port(port == null ? 3000 : Integer.parseInt(port));
        configureApis();
        Spark.before("/", (request, response) -> response.type("application/json"));
        Spark.redirect.any("/", "https://clorabase.tk/apis");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Committing data to drive and cleaning up...");
            for (DatabaseInstance session : DatabaseHandler.getSessions()) {
                System.out.println(session.commit());
            }
        }));
    }

    public static void configureApis() {
        Spark.webSocket("/clorem", DatabaseHandler.class);
        Spark.webSocket("/chat/join", ChatWebSocketHandler.class);
        Spark.path("/chat", ChatServer::start);
        Spark.path("/datastore", ClorabaseDatastore::init);
        Spark.path("/clorastore", ClorastoreDatabase::init);

        Spark.before("/chat/join", (req, res) -> {
            var room = req.queryParams("room");
            var user = req.queryParams("user");
            if (room == null || user == null) {
                res.status(400);
                res.body("Room name is required");
            } else if (ChatServer.rooms.get(room).containsKey(user)) {
                res.status(409);
                res.body("user with name " + user + " already exists");
            } else {
                ChatWebSocketHandler.room = room;
                ChatWebSocketHandler.username = user;
            }
        });
    }
}