import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

public class mainclass {

public static void main(String arg[]) throws UnsupportedEncodingException {

    ArrayList<Entity> entity_list = null;

            IndexFiles indexFiles = new IndexFiles();
            indexFiles.indexttlfile();
        //SparqlHandler sparqlHandler = new SparqlHandler();
        //entity_list = sparqlHandler.fetchEntitiesFromFiles();
        //entity_list = sparqlHandler.fetchClasses();
        // entity_list = sparqlHandler.fetchProperties();
      // entity_list = sparqlHandler.fetchOntologies();

}
}
