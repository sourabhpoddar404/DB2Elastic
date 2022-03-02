import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class IndexFiles {
    private RestHighLevelClient client;
    public void indexttlfile()
    {
        Map<String, String> labelMap = new LinkedHashMap<>();
        String labelFile = "/data-disk/kg-fusion/en/labels_en.ttl";
        try (BufferedReader br = new BufferedReader(new FileReader(labelFile))) {
            String line;
            int i = 0;
            while ((line = br.readLine()) != null && i < 12845254) {
                i++;
                try {
                    String entity = line.substring(line.indexOf("<") + 1, line.indexOf(">"));
                    String label = line.substring(line.indexOf("\"") + 1, line.lastIndexOf("\""));
                    labelMap.put(entity, label);
                } catch (IndexOutOfBoundsException e) {
                    System.out.println(i);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("porque.cs.upb.de", 9400, "http")));

        String dataFile3 = "/data-disk/kg-fusion/en/long_abstracts_en.ttl";

        int numoflines3 = 4935281;
        int i3 = 0;

        Map<String, String> fileEntityMap3 = new LinkedHashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(dataFile3))) {
            String line;
            while ((line = br.readLine()) != null && i3 <numoflines3) {
                IndexRequest request = null;
                i3++;
                String label = "";
                try {
                    if(i3>0)
                    {
                        String entity = line.substring(line.indexOf("<") + 1, line.indexOf(">"));

                        if (!labelMap.containsKey(entity)) {

                            label = entity.substring(entity.indexOf("resource/")+9);

                            //System.out.print(i + " " + entity + " " + label);
                            if(!fileEntityMap3.containsKey(entity))
                                fileEntityMap3.put(entity,label);

                        }

                    }
                } catch (IndexOutOfBoundsException e) {
                    e.printStackTrace();
                    System.out.println(i3);
                }
            }
            System.out.print(fileEntityMap3.size());


        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }



        String dataFile2 = "/data-disk/kg-fusion/en/mappingbased_objects_en.ttl";

        int numoflines2 = 18746176;
        int i2 = 0;

        Map<String, String> fileEntityMap2 = new LinkedHashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(dataFile2))) {
            String line;
            while ((line = br.readLine()) != null && i2 <numoflines2) {
                IndexRequest request = null;
                i2++;
                String label = "";
                try {
                    if(i2>0)
                    {
                        String entity = line.substring(line.indexOf("<") + 1, line.indexOf(">"));

                        if (!labelMap.containsKey(entity)) {

                            label = entity.substring(entity.indexOf("resource/")+9);

                            //System.out.print(i + " " + entity + " " + label);
                            if(!fileEntityMap2.containsKey(entity))
                                fileEntityMap2.put(entity,label);

                        }

                    }
                } catch (IndexOutOfBoundsException e) {
                    e.printStackTrace();
                    System.out.println(i2);
                }
            }
            System.out.print(fileEntityMap2.size());


        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

        String dataFile1 = "/data-disk/kg-fusion/en/mappingbased_literals_en.ttl";

        int numoflines1 = 14388539;
        int i1 = 0;

        Map<String, String> fileEntityMap1 = new LinkedHashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(dataFile1))) {
            String line;
            while ((line = br.readLine()) != null && i1 <numoflines1) {
                IndexRequest request = null;
                i1++;
                String label = "";
                try {
                    if(i1>0)
                    {
                        String entity = line.substring(line.indexOf("<") + 1, line.indexOf(">"));

                        if (!labelMap.containsKey(entity)) {

                            label = entity.substring(entity.indexOf("resource/")+9);

                            //System.out.print(i + " " + entity + " " + label);
                            if(!fileEntityMap1.containsKey(entity))
                                fileEntityMap1.put(entity,label);

                        }

                    }
                } catch (IndexOutOfBoundsException e) {
                    e.printStackTrace();
                    System.out.println(i1);
                }
            }
            System.out.print(fileEntityMap1.size());


        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }


        String dataFile = "/data-disk/kg-fusion/en/persondata_en.ttl";

        int numoflines = 10310107;
        int i = 0;

        Map<String, String> fileEntityMap = new LinkedHashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(dataFile))) {
            String line;
            while ((line = br.readLine()) != null && i <numoflines) {
                IndexRequest request = null;
                i++;
                String label = "";
                try {
                    if(i>0)
                    {
                        String entity = line.substring(line.indexOf("<") + 1, line.indexOf(">"));

                        if (!labelMap.containsKey(entity)) {

                            label = entity.substring(entity.indexOf("resource/")+9);

                            //System.out.print(i + " " + entity + " " + label);
                            if(!fileEntityMap.containsKey(entity))
                                fileEntityMap.put(entity,label);

                        }

                    }
                } catch (IndexOutOfBoundsException e) {
                    e.printStackTrace();
                    System.out.println(i);
                }
            }
            System.out.print(fileEntityMap.size());
            BulkRequest bulkRequest = new BulkRequest();
            int counter =0;
            for (Map.Entry entry: fileEntityMap.entrySet()) {
                String entity = (String) entry.getKey();
                String label = (String) entry.getValue();
                if(!fileEntityMap1.containsKey(entity) && !fileEntityMap2.containsKey(entity) && !fileEntityMap1.containsKey(entity)) {
                    Map<String, Object> jsonMap = new HashMap<>();
                    jsonMap.put("label", label);
                    jsonMap.put("uri", entity);
                    IndexRequest request = request = new IndexRequest(
                            "dbentityindex",
                            "doc");
                    request.source(jsonMap);
                    bulkRequest.add(request);
                    counter++;
                }
                if (counter == 10000) {
                    counter = 0;
                    BulkResponse bulkResponse = client.bulk(bulkRequest);
                    bulkRequest = new BulkRequest();
                }
            }

            BulkResponse bulkResponse = client.bulk(bulkRequest);
        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

    }
}
