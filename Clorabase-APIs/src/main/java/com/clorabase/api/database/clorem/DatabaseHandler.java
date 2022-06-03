package com.clorabase.api.database.clorem;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.java.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@WebSocket()
public class DatabaseHandler {
    public static String DB_ID;
    public static final Map<String, DatabaseInstance> users = new HashMap<>();

    @OnWebSocketConnect
    public void onConnect(Session user) {
        UpgradeRequest request = user.getUpgradeRequest();
        var packageName = request.getHeader("Package");
        var id = request.getHeader("Client-ID");
        var secret = request.getHeader("Client-Secret");
        DB_ID = request.getHeader("DB-ID");
        var token = request.getHeader("Access-Token");
        if (packageName == null || id == null || secret == null || DB_ID == null || token == null) {
            user.close(500, "Credentials not provided");
        } else {
            DatabaseInstance instance = new DatabaseInstance();
            boolean isInitSuccessful = instance.init(id, secret, token, DB_ID, packageName);
            if (isInitSuccessful) {
                users.put(DB_ID, instance);
            } else {
                user.close(500, "Failed to initialize. Are you sure you have the correct credentials?");
            }
        }
    }

    @OnWebSocketClose
    public void onClose(Session user, int statusCode, String reason) throws IOException {
        System.out.println("Closing connection from " + user.getRemoteAddress().getAddress().getHostAddress() + "with reason " + reason);
    }

    @OnWebSocketMessage
    public void onMessage(Session user, String message) throws IOException {
        JSONObject json = new JSONObject(message);
        String type = json.getString("method");
        DatabaseInstance instance = users.get(DB_ID);
        String result = "Invalid request";
        switch (type) {
            case "getData" -> result = instance.getData(json.optString("node"));
            case "putData" -> result = instance.putData(json.optString("node"),json.optString("data"));
            case "addItem" -> result = instance.addItem(json.optString("node"),json.optString("key"),json.optString("value"));
            case "removeItem" -> result = instance.removeItem(json.optString("node"),json.optString("key"), json.optInt("index"));
            case "delete" -> result = instance.delete(json.optString("node"));
            case "query" -> result = instance.query(json.optString("node"),json.optString("query"));
            case "commit" -> result = instance.commit();
            default -> user.close(500, "Invalid method");
        }
        user.getRemote().sendString(result);
    }

    public static DatabaseInstance[] getSessions(){
        return users.values().toArray(new DatabaseInstance[0]);
    }
}
