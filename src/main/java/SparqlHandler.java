import org.apache.http.HttpHost;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.RDFNode;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class SparqlHandler {


    public static Map<String, String> prefixMap;
    private static String commandString = "SELECT ?key1 ?key2\n" +
            "WHERE\n" +
            "{\n" +
            "SELECT DISTINCT  ?key1 ?key2\n" +
            "WHERE\n" +
            "  { ?key1 ?p ?o;\n" +
            "           rdfs:label  ?key2.\n" +
            " FILTER(STRSTARTS(str(?key1 ), \"http://dbpedia.org/resource/\" ))\n" +
            "  }\n" +
            "ORDER BY ?key1\n" +
            "}"; //Read from properties file.
    private static String propertiesString = "SELECT DISTINCT ?key1 ?key2 WHERE {\n" +
            " ?key1 a rdf:Property;\n" +
            " rdfs:label ?key2.\n" +
            " } ";
    private static String classesString = "SELECT DISTINCT ?key1 ?key2 WHERE {\n" +
            " ?key1 a owl:Class .\n" +
            " ?key1 rdfs:label ?key2 .\n" +
            " }";
    private static String ontologyString = "SELECT DISTINCT ?key1 ?key2 WHERE {\n" +
            " ?key1 a owl:ObjectProperty .\n" +
            " ?key1 rdfs:label ?key2 .\n" +
            " }";

    static {
        prefixMap = new HashMap<>();
        prefixMap.put("owl", "http://www.w3.org/2002/07/owl#");
        prefixMap.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        prefixMap.put("vrank", "http://purl.org/voc/vrank#");
        prefixMap.put("xsd", "http://www.w3.org/2001/XMLSchema#");
        prefixMap.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");

    }

    private RestHighLevelClient client;

    public ArrayList<Entity> fetchEntitiesFromFiles() throws UnsupportedEncodingException {

        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("porque.cs.upb.de", 9200, "http")));
        //createIndex("dbentityindexfull");
        String file1 = "/data-disk/kg-fusion/en/genders_en.ttl";
        Map<String, String> labelMap = new HashMap<>();
        String file2 = "/data-disk/kg-fusion/en/labels_en.ttl";
        try (BufferedReader br = new BufferedReader(new FileReader(file2))) {
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
        int i = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(file1))) {
            String line;


            while ((line = br.readLine()) != null) {
                i++;
                String label = "";
                try {
                    String entity = line.substring(line.indexOf("<") + 1, line.indexOf(">"));
                    if (labelMap.containsKey(entity))
                        label = labelMap.get(entity);
                    IndexRequest request = new IndexRequest(
                            "dbentityindexfull",
                            "doc");
                    Map<String, Object> jsonMap = new HashMap<>();
                    jsonMap.put("label", label);
                    jsonMap.put("uri", entity);

                    request.source(jsonMap);
                    IndexResponse indexResponse = client.index(request);
                } catch (IndexOutOfBoundsException | IOException e) {
                    System.out.println(i);
                }

            }


        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }


        //entity_list = generateOutputList(output , keyList, "dbentityindexfull");
        //System.out.println(entity_list.get(0).label);

        return null;
    }

    public Query constructSparqlQuery(String baseURI, String defaultGraph, int limit, int offset, String commandString, Map<String, String> prefixes) {
        ParameterizedSparqlString sparqlQueryHandler = new ParameterizedSparqlString();
        sparqlQueryHandler.setBaseUri(baseURI);
        sparqlQueryHandler.setNsPrefixes(prefixes);
        //TODO: Find a way to handle this by ParameterizedSparql String.
        // Doesnt work with SetLiteral or SetParam(Node) or setIRI. Try other options
        String commandText = commandString;

        sparqlQueryHandler.setCommandText(commandText);


        if (limit > 0) {
            sparqlQueryHandler.append("LIMIT ");
            sparqlQueryHandler.appendLiteral(limit);
        }
        if (offset > 0) {
            sparqlQueryHandler.append("OFFSET ");
            sparqlQueryHandler.appendLiteral(offset);
        }

        Query query = sparqlQueryHandler.asQuery();

        query.addGraphURI(defaultGraph);
        return query;
    }

    public ResultSet executeQuery(String baseURI, Query query) {
        QueryExecution queryExecutionFactory = org.apache.jena.query.QueryExecutionFactory.sparqlService(baseURI, query);
        ResultSet output = queryExecutionFactory.execSelect();
        return output;
    }

    public String getResourceValue(QuerySolution qs, String key) {
        String value = "";
        RDFNode rdfNode = qs.get(key);
        if (rdfNode == null) {

            return "";
        }
        if (rdfNode.isResource() || rdfNode.isURIResource()) {
            value = qs.getResource(key).getURI().toString();
        } else if (rdfNode.isLiteral()) {
            value = qs.getLiteral(key).getString();
        } else {

        }

        return value;
    }

    public ArrayList<Entity> generateOutputList(ResultSet result, ArrayList<String> keyList, String indexName) {
        ArrayList<Entity> entity_list = new ArrayList<Entity>();


        while (result.hasNext()) {
            QuerySolution qs = result.next();
            String entry1 = getResourceValue(qs, keyList.get(0));
            String entry2 = getResourceValue(qs, keyList.get(1));
            if (!entry1.isEmpty() && !entry2.isEmpty()) { //Should not create empty value fields
                Entity entity = new Entity(entry1, entry2);
                entity_list.add(entity);
            }
            IndexRequest request = new IndexRequest(
                    indexName,
                    "doc");
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("label", entry2);
            jsonMap.put("uri", entry1);

            request.source(jsonMap);
            try {
                IndexResponse indexResponse = client.index(request);
            } catch (IOException e) {
                e.printStackTrace();
            }


        }
        return entity_list;
    }

    private void createIndex(String indexName) {
        CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);

        indexRequest.settings(Settings.builder()
                .put("index.blocks.read_only_allow_delete", "false")
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)
        );
        indexRequest.mapping("doc",
                "  {\n" +
                        "	\"doc\" :{\n" +
                        "	\"properties\"	:{\n" +
                        "		\"label\"	:{\n" +
                        "			\"type\" : \"text\",\n" +
                        "			\"fields\":{ \n" +
                        "				\"keyword\": {\n" +
                        "					\"type\":\"keyword\", \n" +
                        "					\"ignore_above\":256\n" +
                        "				}\n" +
                        "			}\n" +
                        "		},\n" +
                        "		\"uri\":{\n" +
                        "			\"type\":\"text\",\n" +
                        "			\"fields\":{\n" +
                        "				\"keyword\":{\n" +
                        "					\"type\":\"keyword\",\n" +
                        "					\"ignore_above\":256\n" +
                        "				}\n" +
                        "			}\n" +
                        "		}\n" +
                        "	}\n" +
                        "}\n" +
                        " 		}",
                XContentType.JSON);

        try {
            CreateIndexResponse createIndexResponse = client.indices().create(indexRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<String> getKeyNames(String commandText) {
        //Read Command Text and get Name of Key1,Key2. THis is important for Custom Queries where user can enter his own choice of name
        ArrayList<String> keyList = new ArrayList<String>();
        String[] splitStr = commandText.split("\\s+");
        for (int i = 0; i < splitStr.length; i++) {
            String token = splitStr[i];
            if (i > 4) {// Query can be SELECT key1, key2 or SELECT UNIQUE/DISTINCT key1,key2
                break;
            } else {
                if (token.contains("?")) {
                    keyList.add(token.substring(1, token.length()));
                }
            }
        }
        return keyList;
    }

    public ArrayList<Entity> fetchEntities() throws UnsupportedEncodingException {

        String baseURI = "http://porque.cs.upb.de:8890/sparql";
        String defaultGraph = "http://www.upb.de/en-dbp2016-10-enriched";
        int limit = 10000;
        ArrayList<Entity> entity_list = null;
        String commandText;
        Map<String, String> prefixes = new HashMap<String, String>();

        prefixes = prefixMap;
        commandText = commandString;

        ArrayList<String> keyList = getKeyNames(commandText);
        for (int offset = 0; offset < 600000; offset += 10000) {
            Query query = constructSparqlQuery(baseURI, defaultGraph, limit, offset, commandText, prefixes);
            ResultSet output = executeQuery(baseURI, query);
            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("porque.cs.upb.de", 9200, "http")));
            if (offset == 0)
                createIndex("dbentityindexfull");
            entity_list = generateOutputList(output, keyList, "dbentityindexfull");
            System.out.println(entity_list.get(0).label);
        }
        return entity_list;
    }


    public ArrayList<Entity> fetchClasses() throws UnsupportedEncodingException {

        String baseURI = "http://porque.cs.upb.de:8890/sparql";
        String defaultGraph = "http://www.upb.de/en-dbp2016-10-enriched";
        int limit = 0;
        int offset = 0;
        String commandText;
        Map<String, String> prefixes = new HashMap<String, String>();

        prefixes = prefixMap;
        commandText = classesString;

        ArrayList<String> keyList = getKeyNames(commandText);
        Query query = constructSparqlQuery(baseURI, defaultGraph, limit, offset, commandText, prefixes);
        ResultSet output = executeQuery(baseURI, query);
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("porque.cs.upb.de", 9200, "http")));
        if (offset == 0)
            createIndex("enricheddbclassindex");
        ArrayList<Entity> classes_list = generateOutputList(output, keyList, "enricheddbclassindex");
        return classes_list;
    }

    public ArrayList<Entity> fetchProperties() throws UnsupportedEncodingException {

        String baseURI = "http://porque.cs.upb.de:8890/sparql";
        String defaultGraph = "http://www.upb.de/en-dbp2016-10-enriched";
        int limit = 0;
        int offset = 0;
        String commandText;
        Map<String, String> prefixes = new HashMap<String, String>();

        prefixes = prefixMap;
        commandText = propertiesString;

        ArrayList<String> keyList = getKeyNames(commandText);
        Query query = constructSparqlQuery(baseURI, defaultGraph, limit, offset, commandText, prefixes);
        ResultSet output = executeQuery(baseURI, query);
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("porque.cs.upb.de", 9200, "http")));
        if (offset == 0)
            createIndex("enricheddbpropertyindex");
        ArrayList<Entity> classes_list = generateOutputList(output, keyList, "enricheddbpropertyindex");
        return classes_list;
    }

    public ArrayList<Entity> fetchOntologies() throws UnsupportedEncodingException {

        String baseURI = "http://porque.cs.upb.de:8890/sparql";
        String defaultGraph = "http://www.upb.de/en-dbp2016-10-enriched";
        int limit = 0;
        int offset = 0;
        String commandText;
        Map<String, String> prefixes = new HashMap<String, String>();

        prefixes = prefixMap;
        commandText = ontologyString;

        ArrayList<String> keyList = getKeyNames(commandText);
        Query query = constructSparqlQuery(baseURI, defaultGraph, limit, offset, commandText, prefixes);
        ResultSet output = executeQuery(baseURI, query);
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("porque.cs.upb.de", 9200, "http")));
        if (offset == 0)
            createIndex("enricheddbontologyindex");
        ArrayList<Entity> classes_list = generateOutputList(output, keyList, "enricheddbontologyindex");
        return classes_list;
    }

}
