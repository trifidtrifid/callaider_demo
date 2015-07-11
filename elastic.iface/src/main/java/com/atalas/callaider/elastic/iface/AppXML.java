package com.atalas.callaider.elastic.iface;

import java.io.BufferedReader;
import java.io.FileInputStream;
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
		        logger.debug(documentType+" described as:"+contentBuilder.toString());
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
				for( Map.Entry<String,Object> obje: ((Map<String,Object>)value).entrySet() ){
					contentBuilder.startObject(name);
					processField(contentBuilder, obje.getValue(), obje.getKey()); 
					contentBuilder.endObject();
				}
			} else { //it's a simple field				
				contentBuilder.field(name, value);
			}
		}
	};
	
	public static void main( String[] args )
    {
    	BasicConfigurator.configure();
    	
    	String argsLine = "";
    	for( String arg: args){argsLine+= " '" + arg +"'";}
    	System.out.println("========= Args.length="+args.length+" args: "+argsLine );
    	
    	Client client = getClient();
		
    	try{
    		InputStream is = args.length == 0 || args[0].equals("-") ? System.in : new FileInputStream(args[0]);
        	
    		FieldMapping mapping = rewriteMapping(client);
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

	private static FieldMapping rewriteMapping(Client client) {
		FieldMapping mapping = loadFieldMapping(); 
		
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

	private static FieldMapping loadFieldMapping() {
		FieldMapping ftFrame = new FieldMapping( FieldMapping.Type.CONTAINER, new HashMap<String, PDMLParser.FieldMapping>());
		ftFrame.lowerLevelMap.put("frame.time", new FieldMapping( FieldMapping.Type.STRING, null));
		ftFrame.lowerLevelMap.put("frame.protocols", new FieldMapping( FieldMapping.Type.STRING, null));
		
		FieldMapping ftIP = new FieldMapping( FieldMapping.Type.CONTAINER, new HashMap<String, PDMLParser.FieldMapping>());
		ftIP.lowerLevelMap.put("ip.version", new FieldMapping( FieldMapping.Type.INT, null));
		ftIP.lowerLevelMap.put("ip.src", new FieldMapping( FieldMapping.Type.STRING, null));
		ftIP.lowerLevelMap.put("ip.dst", new FieldMapping( FieldMapping.Type.STRING, null));
		ftIP.lowerLevelMap.put("ip.proto", new FieldMapping( FieldMapping.Type.INT, null));
		
		FieldMapping ftSCTP = new FieldMapping( FieldMapping.Type.CONTAINER, new HashMap<String, PDMLParser.FieldMapping>());
		ftSCTP.lowerLevelMap.put("sctp.srcport", new FieldMapping( FieldMapping.Type.INT, null));
		ftSCTP.lowerLevelMap.put("sctp.dstport", new FieldMapping( FieldMapping.Type.INT, null));
		ftSCTP.lowerLevelMap.put("sctp.port", new FieldMapping( FieldMapping.Type.INT, null));
		ftSCTP.lowerLevelMap.put("sctp.assoc_index", new FieldMapping( FieldMapping.Type.INT, null));
		
		FieldMapping ftRoot = new FieldMapping( FieldMapping.Type.CONTAINER, new HashMap<String, PDMLParser.FieldMapping>());
		ftRoot.lowerLevelMap.put( "frame", ftFrame );
		ftRoot.lowerLevelMap.put( "sctp", ftSCTP );
		ftRoot.lowerLevelMap.put( "ip", ftIP );
		return ftRoot;
	}
}
