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
        /*client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("porque.cs.upb.de", 9400, "http")));*/


        String dataFile = "/data-disk/kg-fusion/en/anchor_text_en.ttl";
        BulkRequest bulkRequest = new BulkRequest();
        int numoflines = 163264419;
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

            int counter =0;
            for (Map.Entry entry: fileEntityMap.entrySet()) {
                String entity = (String) entry.getKey();
                String label = (String) entry.getValue();
                Map<String, Object> jsonMap = new HashMap<>();
                jsonMap.put("label", label);
                jsonMap.put("uri", entity);
                IndexRequest request = request = new IndexRequest(
                        "dbentityindex",
                        "doc");
                request.source(jsonMap);
                bulkRequest.add(request);
                counter++;
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
