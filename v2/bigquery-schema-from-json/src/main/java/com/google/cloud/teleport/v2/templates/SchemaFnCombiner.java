package com.google.cloud.teleport.v2.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;
import org.apache.beam.sdk.transforms.Combine;

public class SchemaFnCombiner extends Combine.CombineFn<TableRow, SchemaFnCombiner.SchemaAccumulator, TreeMap<String, HashMap<String, Long>>> {

    public static final String FIELD_STRING = "STRING";
    public static final String FIELD_INTEGER = "INTEGER";
    public static final String FIELD_DOUBLE = "DOUBLE";
    public static final String FIELD_RECORD = "RECORD";
    public static final String FIELD_ARRAY = "ARRAY";



    public static class SchemaAccumulator implements Serializable {

        //Can implement more data quality info with counters
        TreeMap<String, HashMap<String, Long>> schemaStatus = new TreeMap<>();
        int count = 0;
    }

    @Override
    public SchemaAccumulator createAccumulator() {
        return new SchemaAccumulator();
    }


    private void incrementTypeStatus(HashMap<String, Long> keyStatsMap, String type) {
        if (!keyStatsMap.containsKey(type)) {
            keyStatsMap.put(type, new Long(1));
        } else {
            keyStatsMap.put(type, keyStatsMap.get(type) + 1);
        }
    }

    public void getSchemaField(String path, String key, Object value, SchemaAccumulator schemaAccumulator) {
        String currentPath = MoreObjects.firstNonNull(path, "");
        if (currentPath != "") {
            currentPath += "." + key;
        } else {
            currentPath = key;
        }
        //Initialize key
        if (!schemaAccumulator.schemaStatus.containsKey(currentPath)) {
            schemaAccumulator.schemaStatus.put(currentPath, new HashMap<String, Long>());
        }

        //Status map for the key
        HashMap<String, Long> keyStatsMap = schemaAccumulator.schemaStatus.get(currentPath);

        if (value instanceof String) {
            incrementTypeStatus(keyStatsMap, FIELD_STRING);
        } else if (value instanceof Integer) {
            incrementTypeStatus(keyStatsMap, FIELD_INTEGER);
        } else if (value instanceof Double) {
            incrementTypeStatus(keyStatsMap, FIELD_DOUBLE);
        } else if (value instanceof LinkedHashMap) {
            incrementTypeStatus(keyStatsMap, FIELD_RECORD);
            LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap<String, Object>) value;
            for (String linkedKey : ((LinkedHashMap<String, Object>) value).keySet()) {
                getSchemaField(currentPath, linkedKey, linkedHashMap.get(linkedKey), schemaAccumulator);
            }
        } else if (value instanceof ArrayList) {
            List<Object> arrayList = (ArrayList<Object>) value;
            if (arrayList.size() > 0) {
                Object o = arrayList.get(0);
                System.out.println("Array Type Class:" + o.getClass());
                if (o instanceof LinkedHashMap) {
                    incrementTypeStatus(keyStatsMap, FIELD_ARRAY + ":" + FIELD_RECORD);
                    for (String arrayStructKey : ((LinkedHashMap<String, Object>) o).keySet()) {
                        getSchemaField(currentPath, arrayStructKey, o, schemaAccumulator);
                    }
                } else if (o instanceof String) {
                    incrementTypeStatus(keyStatsMap, FIELD_ARRAY + ":" + FIELD_STRING);
                } else if (o instanceof Integer) {
                    incrementTypeStatus(keyStatsMap, FIELD_ARRAY + ":" + FIELD_INTEGER);
                } else if (o instanceof Double) {
                    incrementTypeStatus(keyStatsMap, FIELD_ARRAY + ":" + FIELD_DOUBLE);
                }
            }
        }
    }


    /**
     * Adds the given input value to the given accumulator, returning the new accumulator value.
     *
     * @param mutableAccumulator may be modified and returned for efficiency
     * @param tableRow           should not be mutated
     */
    @Override
    public SchemaAccumulator addInput(SchemaAccumulator mutableAccumulator, TableRow tableRow) {

//        TableRow tableRow = kvTableRow.getValue();
        for (String key : tableRow.keySet()) {
//                System.out.println("Key: " + key + " Value: " + tableRow.get(key)  + " Class: "+ tableRow.get(key).getClass());
            getSchemaField(null, key, tableRow.get(key), mutableAccumulator);
        }

        mutableAccumulator.count += 1;

        return mutableAccumulator;
    }

    @Override
    public SchemaAccumulator mergeAccumulators(Iterable<SchemaAccumulator> accums) {
        SchemaAccumulator merged = createAccumulator();
        for (SchemaAccumulator schemaAccumulator : accums) {

            for (String key : schemaAccumulator.schemaStatus.keySet()) {
                if (merged.schemaStatus.get(key) != null) {
                    HashMap<String, Long> accMap = schemaAccumulator.schemaStatus.get(key);
                    HashMap<String, Long> resultMap = merged.schemaStatus.get(key);
                    for (String keyMap : accMap.keySet()) {
                        if (resultMap.containsKey(keyMap)) {
                            long total = resultMap.get(keyMap) + accMap.get(keyMap);
                            resultMap.put(keyMap, total);
                        } else {
                            resultMap.put(keyMap, accMap.get(keyMap));
                        }
                    }
                } else {
                    //Don't have any value for the key, retrieve from accumulator
                    merged.schemaStatus.put(key, schemaAccumulator.schemaStatus.get(key));
                }

            }
            merged.count += schemaAccumulator.count;
        }
        return merged;
    }

    @Override
    public TreeMap<String, HashMap<String, Long>> extractOutput(SchemaAccumulator schemaAccumulator) {
        // Extract Table Schema from all Accumulators
        System.out.println("Total:" + schemaAccumulator.count + "\nSchema: \n" + schemaAccumulator.schemaStatus.toString());
        return schemaAccumulator.schemaStatus;
    }
}
