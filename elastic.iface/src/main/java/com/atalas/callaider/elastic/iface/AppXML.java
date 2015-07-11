package com.atalas.callaider.elastic.iface;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.atalas.callaider.elastic.iface.PDMLParser.FieldMapping;
import com.atalas.callaider.elastic.iface.PDMLParser.FieldMapping.Type;
import com.atalas.callaider.elastic.iface.PDMLParser.PacketListener;

public class AppXML {
	
	private static Logger logger = Logger.getLogger(AppXML.class.getName()) ;
	
	public final static String indexName = "network";
	public final static String documentType = "packet";
	
	private static Client getClient() {
        final ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
        TransportClient transportClient = new TransportClient(settings)
        	.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        return transportClient;
    }
	
	private static class PacketProcessor implements PacketListener {
		private final Client client;
		public PacketProcessor( Client client){
			this.client = client;			
		}
		
		@Override
		public void processNextPacket(Map<String, Object> packet) {
			
	        final IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName, documentType, null);
	        try{
		        final XContentBuilder contentBuilder = jsonBuilder().startObject().prettyPrint();
		        for( Entry<String, Object> field: packet.entrySet()){
		        	Object value = field.getValue();
		        	String name = field.getKey();		        	
					processField(contentBuilder, value, name);
		        }
		        contentBuilder.endObject();
		        indexRequestBuilder.setSource(contentBuilder);
		        logger.debug(documentType+" described as:"+contentBuilder.string());
		        IndexResponse actionGet = indexRequestBuilder.execute().actionGet();
		        logger.debug(documentType+"created with ID:"+actionGet.getId());
	        } catch (Exception e){
	        	e.printStackTrace();
	        }
		}

		private void processField(final XContentBuilder contentBuilder,
				Object value, String name) throws IOException {
			if( value instanceof List ) {
				for( Object obj: (List)value){
					processField(contentBuilder, obj, name); 
				}
			} if( value instanceof Map ) {
				contentBuilder.startObject(name);
				for( Map.Entry<String,Object> obje: ((Map<String,Object>)value).entrySet() ){					
					processField(contentBuilder, obje.getValue(), obje.getKey()); 					
				}
				contentBuilder.endObject();
			} else { //it's a simple field				
				contentBuilder.field(name, value);
			}
		}
	};
	
	public static void main( String[] args )
    {
		if( args.length < 1 ){
			System.err.println("USage java -jar xxx.jar <input file name or - (stdin)> <fields.map.file>");
		}
    	BasicConfigurator.configure();
    	
    	Client client = getClient();
		try{
    		InputStream is = args[0].equals("-") ? System.in : new FileInputStream(args[0]);
			FieldMapping mapping = rewriteMapping(client, args[1]);
     		PDMLParser pdmlP = new PDMLParser(is, mapping);
     		PacketProcessor pp = new PacketProcessor(client);
    		pdmlP.parse(pp);
    		
    	} catch(Exception io){
    		io.printStackTrace();
    		System.out.println("Error while read: "+io.getMessage());
    	} finally {
    		client.close();
    	}
    }

	private static FieldMapping rewriteMapping(Client client, String fileldsFileName) throws IOException {
		FieldMapping mapping = loadFieldMapping(fileldsFileName); 
		
		final IndicesExistsResponse res = client.admin().indices().prepareExists(indexName).execute().actionGet();
		if (res.isExists()) {
		    final DeleteIndexRequestBuilder delIdx = client.admin().indices().prepareDelete(indexName);
		    delIdx.execute().actionGet();
		}

		final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);

		// MAPPING GOES HERE
		   		
		MappingCreator.createMapping(documentType, mapping);
		String mappincDoc = MappingCreator.createMapping(documentType, mapping);
		createIndexRequestBuilder.addMapping( documentType, mappincDoc);
		logger.debug(documentType+" mapping described as:"+mappincDoc);
		// MAPPING DONE
		createIndexRequestBuilder.execute().actionGet();
		return mapping;
	}

	private static FieldMapping loadFieldMapping( String fileName) throws IOException {
		
		BufferedReader br = 
                new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
		String line;
		FieldMapping ftRoot = new FieldMapping( FieldMapping.Type.CONTAINER, new HashMap<String, PDMLParser.FieldMapping>());
		while( null!=(line=br.readLine())){
			parseString( line, ftRoot );
		}
		return ftRoot;
		
	}
	private static void parseString( String descLine, FieldMapping currentMapping ){
		String[] parts = descLine.split("[ ]");
		postThePart(currentMapping, parts, 0);		
	}

	private static void postThePart(FieldMapping currentMapping, String[] parts, int offset) {
		if( parts.length - offset == 2 ){ //field name and field type
			currentMapping.lowerLevelMap.put( "\"\"".equals(parts[offset]) ? "x" : parts[offset], new FieldMapping(Type.valueOf(parts[offset+1]), null));
		} if( parts.length - offset > 2 ){
			FieldMapping childMap = currentMapping.lowerLevelMap.get(parts[offset]);
			if( null == childMap ){
				childMap = new FieldMapping(Type.CONTAINER, new HashMap<String, FieldMapping>());
				currentMapping.lowerLevelMap.put(parts[offset],childMap);				
			}
			postThePart(childMap, parts, offset+1);
		}
	}
}
