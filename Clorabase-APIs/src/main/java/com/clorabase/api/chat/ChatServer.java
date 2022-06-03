package com.clorabase.api.chat;

import org.eclipse.jetty.websocket.api.Session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

import spark.Spark;

public class ChatServer {
    public static Map<String, Map<String, Session>> rooms = new HashMap<>();
    public static Map<String, String> admins = new HashMap<>();

    public static void start() {
        Spark.get("/create", (req, res) -> {
            var id = req.queryParams("room");
            if (id == null) {
                return "Room id (query parameter) is required";
            } else {
                if (rooms.containsKey(id)) {
                    res.status(400);
                    return "Room already exists";
                } else {
                    rooms.put(id,new HashMap<>());
                    res.status(201);
                    var admin = Long.toHexString(new Random().nextLong());
                    admins.put(id, admin);
                    return admin;
                }
            }
        });

        Spark.get("/:room/delete", (req, res) -> {
            var room = req.params("room");
            var user = req.queryParams("user");
            var admin = req.queryParams("admin");
            Objects.requireNonNull(admin, "Admin is required");
            if (rooms.containsKey(room)) {
                if (admin.equals(admins.get(room))) {
                    if (user == null) {
                        rooms.remove(room);
                        res.status(200);
                        return "Room successfully closed.";
                    } else if (rooms.get(room).containsKey(user)) {
                        rooms.get(room).remove(user).close(1000, "User kicked by admin");
                        res.status(200);
                        return "User kicked successfully";
                    } else {
                        res.status(404);
                        return "No such user";
                    }
                } else {
                    res.status(403);
                    return "Wrong admin ID";
                }
            } else {
                res.status(404);
                return "Room not exist";
            }
        });

        Spark.get("/:room/members", (req, res) -> {
            var room = req.params("room");
            if (room == null) {
                res.status(400);
                return "Room id is required";
            } else {
                res.status(200);
                res.type("application/json");
                return "{\"members\":" + rooms.get(room).keySet().toString() + "}";
            }
        });
    }
}
