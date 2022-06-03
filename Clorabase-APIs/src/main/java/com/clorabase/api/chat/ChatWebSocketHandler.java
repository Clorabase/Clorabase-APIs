package com.clorabase.api.chat;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketFrame;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.api.extensions.Frame;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

@WebSocket
public class ChatWebSocketHandler {
    public static String room;
    public static String username;

    @OnWebSocketConnect
    public void onConnect(Session user) throws IOException {
        ChatServer.rooms.get(room).put(username, user);
        broadcast(user, "Joined the room");
    }

    @OnWebSocketClose
    public void onClose(Session user, int statusCode, String reason) throws IOException {
        var params = user.getUpgradeRequest().getParameterMap();
        room = params.get("room").get(0);
        username = params.get("user").get(0);
        Map<String, Session> users = ChatServer.rooms.get(room);
        users.remove(username);
        if (users.size() == 0){
            ChatServer.rooms.remove(room);
        }
        broadcast(user, "Left the room");
    }

    @OnWebSocketMessage
    public void onMessage(Session user, String message) throws IOException {
        room = user.getUpgradeRequest().getParameterMap().get("room").get(0);
        username = user.getUpgradeRequest().getParameterMap().get("user").get(0);
        if (message.startsWith("[PM:")){
            var userTo = message.substring(4, message.indexOf("]"));
            var msg = message.substring(message.indexOf("]") + 1);
            if (ChatServer.rooms.get(room).containsKey(userTo)){
                ChatServer.rooms.get(room).get(userTo).getRemote().sendString(username + ": " + msg);
            }
        } else {
            broadcast(user, message);
        }
    }

    private void broadcast(Session user, String message) throws IOException {
        for (Session session : ChatServer.rooms.get(room).values()) {
            if (session != user)
                session.getRemote().sendString(username + ":" + message);
        }
    }
}
