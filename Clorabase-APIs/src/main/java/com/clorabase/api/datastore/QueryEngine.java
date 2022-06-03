package com.clorabase.api.datastore;

import org.java.json.JSONArray;
import org.java.json.JSONException;
import org.java.json.JSONObject;

import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;

public class QueryEngine {

    public static JSONArray queryAnd(JSONArray array, String query, int limit) {
        var conditions = query.substring(1, query.length() - 1).split("&");
        var result = array;
        for (String condition : conditions) {
            result = filter(result, condition, limit);
        }
        return result;
    }

    public static JSONArray queryOr(JSONArray array, String query, int limit) {
        var conditions = query.substring(1, query.length() - 1).split("\\|");
        JSONArray result = new JSONArray();
        for (String condition : conditions) {
            result.putAll(filter(array, condition, limit));
        }
        return result;
    }

    private static JSONArray filter(JSONArray array, String condition, int limit) {
        JSONArray result = new JSONArray();
        limit = limit == 0 ? array.length() : limit;
        for (int i = 0; i < Math.min(limit, array.length()); i++) {
            var object = array.getJSONObject(i);
            if (condition.contains("=")) {
                var field = condition.split("=")[0];
                var value = condition.split("=")[1];
                var fieldValue = object.get(field);
                if (fieldValue instanceof Number) {
                    if (((Number) fieldValue).doubleValue() == Double.parseDouble(value)) {
                        result.put(object);
                    }
                } else {
                    if (fieldValue.equals(value)) {
                        result.put(object);
                    }
                }
            } else if (condition.contains(">")) {
                var field = condition.split(">")[0];
                var value = Double.parseDouble(condition.split(">")[1]);
                if (object.getNumber(field).doubleValue() > value)
                    result.put(object);
            } else if (condition.contains("<")) {
                var field = condition.split("<")[0];
                var value = Double.parseDouble(condition.split("<")[1]);
                if (object.getNumber(field).doubleValue() < value)
                    result.put(object);
            } else if (condition.contains("!")) {
                var field = condition.split("!")[0];
                var value = condition.split("!")[1];
                var fieldValue = object.get(field);
                if (fieldValue instanceof Number) {
                    if (((Number) fieldValue).doubleValue() != Double.parseDouble(value)) {
                        result.put(object);
                    }
                } else {
                    if (!fieldValue.equals(value)) {
                        result.put(object);
                    }
                }
            } else if (condition.contains("~")) {
                var field = condition.split("~")[0];
                var values = condition.split("~")[1].replace("[", "").replace("]", "").split(",");
                var fieldValue = object.get(field);
                for (String value : values) {
                    if (fieldValue instanceof Number) {
                        if (((Number) fieldValue).doubleValue() == Double.parseDouble(value)) {
                            result.put(object);
                        }
                    } else {
                        if (fieldValue.equals(value)) {
                            result.put(object);
                        }
                    }
                }
            } else
                throw new IllegalArgumentException("Invalid query condition: " + condition);
        }
        return result;
    }

    public static JSONArray ordered(JSONArray array, String order) {
        try {
            var sorted = array.toList()
                    .stream()
                    .map(o -> new JSONObject((Map) o))
                    .sorted(((o1, o2) -> (int) (o1.getNumber(order).doubleValue() - o2.getNumber(order).doubleValue())))
                    .toList();
            return new JSONArray(sorted);
        } catch (JSONException e){
            throw new NumberFormatException("Invalid order field: " + order + " (must be a number)");
        }
    }
}
