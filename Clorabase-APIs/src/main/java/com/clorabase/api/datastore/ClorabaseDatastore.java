package com.clorabase.api.datastore;

import org.java.json.JSONException;
import org.java.json.JSONObject;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.HttpException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import spark.Spark;

public class ClorabaseDatastore {
    protected static GHRepository repo;
    protected static Map<String, Integer> countMap = new HashMap<>();

    static {
        try {
            repo = GitHub.connectUsingOAuth("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").getRepository("Clorabase-databases/Datastore");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void init() {
        Spark.get("/:name", (req, res) -> {
            var node = req.queryParams("key");
            var name = req.params("name");
            var content = getFileContent("Datastore", "documents/" + name);
            System.out.println("content is : " + content);
            if (node == null)
                return content;
            else {
                JSONObject object = new JSONObject(content);
                if (node.contains(".")) {
                    var path = node.substring(0, node.lastIndexOf("."));
                    var key = node.substring(node.lastIndexOf(".") + 1);
                    for (var child : path.split("\\.")) {
                        object = object.getJSONObject(child);
                    }
                    return object.get(key);
                } else
                    return object.get(node);
            }
        });

        Spark.put("/:name", (request, res) -> {
            try {
                var json = request.body();
                var name = request.params("name");
                var doc = repo.getFileContent("documents/" + name);
                var content = new String(doc.read().readAllBytes());

                if (json == null || name == null) {
                    res.status(400);
                    return "ID or body missing !";
                } else if (content.getBytes().length > 1024 * 1024) {
                    res.status(400);
                    return "Datastore size exceeds 1 MB. You can only store data up to 1 MB";
                } else {
                    JSONObject newData = new JSONObject(json);
                    JSONObject oldData = new JSONObject(content);
                    var iterator = newData.keys();
                    while (iterator.hasNext()) {
                        var key = iterator.next();
                        oldData.put(key, newData.get(key));
                    }
                    doc.update(oldData.toString(), "Updating data");
                    res.status(200);
                    return null;
                }
            } catch (Exception e) {
                e.printStackTrace();
                return "Error";
            }
        });

        Spark.post("/create", (req, res) -> {
            var name = req.queryParams("name");
            var data = req.body();
            var count = countMap.getOrDefault(req.ip(), 0);
            if (name == null) {
                res.status(400);
                return "No datastore name provided";
            } else if (count == 5) {
                res.status(403);
                return "You have already created many datastore. Delete some to create more";
            } else {
                repo.createContent(data, "Creating datastore document", "documents/" + name);
                countMap.put(req.ip(), ++count);
                res.status(201);
                return "null";
            }
        });

        Spark.delete("/:id", ((request, response) -> {
            var id = request.params("id");
            var count = countMap.getOrDefault(request.ip(), 0);
            if (id == null) {
                response.status(400);
                return "No datastore ID provided.";
            } else {
                countMap.put(request.ip(), --count);
                repo.getFileContent("documents/" + id).delete("Database deleted");
                response.status(200);
                return null;
            }
        }));

        Spark.exception(Exception.class, (exception, request, response) -> {
            if (exception instanceof JSONException) {
                response.status(500);
                response.body("Document data corrupted. Deleting document...");
                try {
                    repo.getFileContent("documents/" + request.params("name")).delete("Deleting corrupted doc");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else if (exception instanceof FileNotFoundException) {
                response.status(404);
                response.body("The database or document does not exist");
            } else if (exception instanceof HttpException) {
                response.status(400);
                response.body("The document your are creating already exists");
            } else {
                response.status(500);
                response.body("Something horribly gone wrong. This should never happen. Please create an issue regarding this error at " +
                        "https://github.com/ErrorxCode/Clorabase-APIs/issues. Error details :-\n" + exception.getLocalizedMessage());
            }
        });
    }

    public static String getFileContent(String repo, String path) throws IOException {
        var connection = new URL("https://raw.githubusercontent.com/Clorabase-databases/" + repo + "/main/" + path).openConnection();
        var in = connection.getInputStream();
        return new String(in.readAllBytes());
    }
}
