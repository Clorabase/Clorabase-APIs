package com.clorabase.api.database.clorem;

import org.java.json.JSONArray;
import org.java.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import apis.xcoder.easydrive.EasyDrive;
import db.clorabase.clorem.Clorem;
import db.clorabase.clorem.CloremDatabaseException;
import db.clorabase.clorem.Node;

public class DatabaseInstance {
    public EasyDrive drive;
    public boolean success = true;
    public Clorem clorem;
    public Node root;
    public String fileId;
    public File DB_DIR;

    protected boolean init(String id,String secret,String token,String fileId){
        try {
            this.fileId = fileId;
            drive = new EasyDrive(id, secret, token);
            DB_DIR = new File(System.getProperty("user.home"),"Databases/" + fileId);
            if (!DB_DIR.exists() && !DB_DIR.mkdirs())
                return false;
            if (fileId == null) {
                return false;
            } else {
                drive.download(fileId,DB_DIR.getPath(), new EasyDrive.ProgressListener() {
                    @Override
                    public void onProgress(int percentage) {

                    }

                    @Override
                    public void onFinish(String fileId) {
                        clorem = Clorem.getInstance(DB_DIR, "clorabase");
                        root = clorem.getDatabase();
                    }

                    @Override
                    public void onFailed(Exception e) {
                        e.printStackTrace();
                        success = false;
                    }
                });
            }
            return success;
        } catch (GeneralSecurityException | IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    protected String putData(String node, String data) {
        if (data == null) {
            return "{\"error\":\"data is null\"}";
        } else {
            Map<String, Object> map = new HashMap<>();
            JSONObject object = new JSONObject(data);
            Iterator<String> iterator = object.keys();
            while (iterator.hasNext()) {
                var key = iterator.next();
                var value = object.get(key);
                map.put(key, value);
            }
            map.put("_address",node);
            if (node == null || node.equals(""))
                root.put(map);
            else
                root.node(node).put(map);

            return "{\"status\":\"success\"}";
        }
    }

    protected String addItem(String node,String key,Object value){
        if (key == null || value == null) {
            return "{\"error\":\"key or value is null\"}";
        } else {
            try {
                if (value instanceof String str) {
                    if (node == null) {
                        root.addItem(key, str);
                    } else
                        root.node(node).addItem(key,str);
                } else if (value instanceof Integer number) {
                    if (node == null) {
                        root.addItem(key, number);
                    } else
                        root.node(node).addItem(key, number);
                } else
                    return "{\"error\":\"value is not a string or number\"}";

                return "{\"status\":\"success\"}";
            } catch (CloremDatabaseException e) {
                return "{\"error\":\"" + e.getMessage() + "\"}";
            }
        }
    }

    protected String removeItem(String node,String key, int index) {
        try {
            if (key == null) {
                return "{\"error\":\"node or key is null\"}";
            } else {
                if (node == null) {
                    root.removeItem(key, index);
                } else
                    root.node(node).removeItem(key,index);

                return "{\"status\":\"success\"}";
            }
        } catch (Exception e){
            return "{\"error\":\"Either the key is invalid or index is not a number\"}";
        }
    }

    protected String delete(String node){
        if (node == null) {
            return new JSONObject("{\"error\":\"node parameter not specified\"}").toString();
        } else {
            if (node.startsWith("/"))
                node = node.substring(1);
            String[] splits = node.split("/");
            if (splits.length == 1) {
                root.delete(node);
                return "{\"status\":\"200 OK\"}";
            } else {
                var nodeName = splits[splits.length - 1];
                var parent = node.substring(0, node.indexOf(nodeName));
                return root.node(parent).delete(nodeName) ? "{\"status\":\"success\"}" : "{\"error\":\"node not found\"}";
            }
        }
    }

    protected String getData(String node) {
        if (node == null || node.equals("")) {
            try {
                return clorem.getDatabaseAsJson();
            } catch (CloremDatabaseException e) {
                return e.getMessage();
            }
        } else {
            return new JSONObject(root.node(node).getData()).toString();
        }
    }

    public String commit() {
        root.commit();
        try {
            drive.updateFile(fileId, new File(DB_DIR, "clorabase.db")).getResult(5);
            return "{\"status\":\"success\"}";
        } catch (Exception e) {
            e.printStackTrace();
            return "{\"error\":\"Unable to push file to the drive\"}";
        }
    }

    protected String query(String grandparent,String query){
        if (query == null) {
            return new JSONObject("{\"error\":\"query is null\"}").toString();
        } else {
            Node queryNode = grandparent == null ? root : root.node(grandparent);
            List<Node> nodes = queryNode.query().fromQuery(query);
            JSONArray array = new JSONArray();
            for (Node node : nodes) {
                Map<String, Object> data = node.getData();
                if (!data.isEmpty())
                    array.put(data);
            }
            return array.toString();
        }
    }
}
