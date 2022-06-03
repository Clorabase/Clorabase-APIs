package com.clorabase.api.database.clorastore;


import com.clorabase.api.datastore.QueryEngine;

import org.java.json.JSONArray;
import org.java.json.JSONException;
import org.java.json.JSONObject;
import org.kohsuke.github.GHContent;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.HttpException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import spark.Spark;

public class ClorastoreDatabase {
    public static final String OWNER = "Clorabase-databases/";

    public static void init() {
        Spark.post("/provision/:name", (req, res) -> {
            var name = req.params("name");
            var token = req.headers("Authorization").replace("Basic ", "");

            if (name == null) {
                res.status(400);
                return "Missing database name. Please provide the name of the database to be created.";
            } else {
                GitHub.connectUsingOAuth(token).createRepository(name).create();
                res.status(201);
                return "Database Successfully created in your github account";
            }
        });

        Spark.post("/:db", (req, res) -> {
            var db = req.params("db");
            var path = req.queryParams("path");
            var comment = req.queryParams("comment");
            var token = req.headers("Authorization").replace("Basic ", "");
            var content = new JSONObject(req.body());
            var github = GitHub.connectUsingOAuth(token).getRepository(OWNER + db);

            if (path == null) {
                res.status(400);
                return "Document path parameter missing";
            } else {
                github.createContent(content.toString(), comment == null ? "Creating new document" : comment, path);
                res.status(201);
                return "Successfully created";
            }
        });

        Spark.patch("/:db", (req, res) -> {
            var db = req.params("db");
            var path = req.queryParams("path");
            var comment = req.queryParams("comment");
            var token = req.headers("Authorization").replace("Basic ", "");
            var newJson = new JSONObject(req.body());
            var github = GitHub.connectUsingOAuth(token).getRepository(OWNER + db);
            if (path == null) {
                res.status(400);
                return "Document path parameter missing";
            } else {
                var file = github.getFileContent(path);
                var oldJson = new JSONObject(new String(file.read().readAllBytes()));
                var itr = newJson.keys();
                while (itr.hasNext()) {
                    var key = itr.next();
                    oldJson.put(key, newJson.get(key));
                }
                file.update(oldJson.toString(), comment == null ? "Updated document" : comment);
                res.status(201);
                return "Successfully created";
            }
        });

        Spark.delete("/:db", (req, res) -> {
            var db = req.params("db");
            var path = req.queryParams("path");
            var comment = req.queryParams("comment");
            var token = req.headers("Authorization").replace("Basic ", "");
            var github = GitHub.connectUsingOAuth(token).getRepository(OWNER + db);
            if (path == null) {
                github.delete();
            } else {
                try {
                    github.getFileContent(path).delete(comment == null ? "Deletion" : comment);
                } catch (HttpException e) {
                    deleteRecursive(github, github.getDirectoryContent(path));
                }
            }
            res.status(200);
            return "Successfully deleted";
        });

        Spark.get("/:db", (req, res) -> {
            var db = req.params("db");
            var path = req.queryParams("path");
            var token = req.headers("Authorization").replace("Basic ", "");

            if (path == null) {
                res.status(400);
                return "Document path parameter missing";
            } else {
                var github = GitHub.connectUsingOAuth(token).getRepository(OWNER + db);
                res.status(200);
                try {
                    return getFileContent(github.getName(), path);
                } catch (FileNotFoundException e) {
                    var files = github.getDirectoryContent(path);
                    var query = req.queryParams("query");
                    var order = req.queryParams("orderBy");

                    JSONArray array = new JSONArray();
                    files.stream().filter(GHContent::isFile).map(file -> {
                        try {
                            return new JSONObject(new String(file.read().readAllBytes()));
                        } catch (IOException ioException) {
                            return "['ERROR READING FILE']";
                        }
                    }).forEach(array::put);
                    if (order != null)
                        array = QueryEngine.ordered(array, order);
                    if (query == null) {
                        return array.toString(3);
                    } else {
                        int limit = Integer.parseInt(req.queryParams("limit") == null ? "0" : req.queryParams("limit"));
                        if (query.startsWith("'") && query.endsWith("'")) {
                            JSONArray result;
                            if (query.contains("|") && query.contains("&")) {
                                res.status(400);
                                return "Query parameter cannot contain both | and &, complex query is not supported";
                            } else if (query.contains("|")) {
                                res.status(200);
                                result = QueryEngine.queryOr(array, query, limit);
                            } else if (query.contains("&")) {
                                res.status(200);
                                result = QueryEngine.queryAnd(array, query, limit);
                            } else {
                                res.status(200);
                                result = QueryEngine.queryAnd(array, query, limit);
                            }
                            if (order == null)
                                return result.toString(3);
                            else
                                return QueryEngine.ordered(result, order).toString(3);
                        } else {
                            res.status(400);
                            return "Query parameter must be enclosed in single quotes";
                        }
                    }
                }
            }
        });

        Spark.get("/api/quota", (req, res) -> {
            var token = req.headers("Authorization").replace("Basic ", "");
            var limit = GitHub.connectUsingOAuth(token).getRateLimit();
            var json = new JSONObject();
            json.put("limit", limit.getLimit());
            json.put("remaining", limit.getRemaining());
            json.put("reset", limit.getResetDate());
            res.status(200);
            return limit;
        });

        Spark.exception(Exception.class, (exception, request, response) -> {
            if (exception instanceof JSONException) {
                response.status(400);
                response.body("Request body is not a valid json object");
            } else if (exception instanceof FileNotFoundException) {
                response.status(404);
                response.body("No document exists at path : " + request.queryParams("path") + ". If you want to create new document, please use POST method");
            } else if (exception instanceof NullPointerException) {
                response.status(401);
                response.body("Authorization header missing");
            } else if (exception instanceof NumberFormatException) {
                response.status(400);
                response.body("Invalid limit or orderBy parameter. They both must be integers");
            } else if (exception instanceof ArrayIndexOutOfBoundsException || exception instanceof IllegalArgumentException) {
                exception.printStackTrace();
                response.status(400);
                response.body("Invalid query syntax");
            } else if (exception instanceof HttpException aex) {
                var code = aex.getResponseCode();
                response.status(aex.getResponseCode());
                response.body(code == 401 ? "Invalid authorization token"
                        : code == 422 ? "Document already exists. Please use PUT method to update it"
                        : code == 200 ? "Path does not denotes a document"
                        : code == 409 ? "A file already exists with the same name of which you are creating collection"
                        : "An database already exists with this name in your account");
            } else {
                response.status(500);
                response.body(exception.getMessage());
            }
        });
    }

    private static void deleteRecursive(GHRepository repo, List<GHContent> files) {
        files.forEach(file -> {
            try {
                if (file.isDirectory())
                    deleteRecursive(repo, repo.getDirectoryContent(file.getPath()));
                else
                    file.delete("Deleting collection");
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        });
    }

    private static String getFileContent(String repo, String path) throws IOException {
        var connection = new URL("https://raw.githubusercontent.com/Clorabase-databases/" + repo + "/main/" + path).openConnection();
        var in = connection.getInputStream();
        var str = new String(in.readAllBytes());
        if (str.startsWith("{"))
            return str;
        else
            throw new FileNotFoundException("Path does not denote a document");
    }
}
