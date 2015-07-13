package com.atalas.callaider.elastic.iface;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class AppXML {
	
	static Logger logger = Logger.getLogger(AppXML.class.getName()) ;
	
	public final String indexName = "network";
	public final String documentType = "packet";
	private Map<String,ProtocolContainer.Description>  protocolMap;
	
	private static Client getClient() {
        final ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
        TransportClient transportClient = new TransportClient(settings)
        	.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        return transportClient;
    }
	
	public static void main( String[] args )
    {
		AppXML app = new AppXML();
		if( args.length < 2 ){
			System.err.println("USage java -jar xxx.jar <input file name or - (stdin)> <fields.map.file>");
			return;
		}

    	BasicConfigurator.configure();
    	
    	String argsLine = "";
    	for( String arg: args){argsLine+= " '" + arg +"'";}
    	System.out.println("========= Args.length="+args.length+" args: "+argsLine );
    	
    	Client client = getClient();

		try{
    		app.loadMapping(client, args[1]);
    		InputStream is = args[0].equals("-") ? System.in : new FileInputStream(args[0]);
			
    		PDMLParser pdmlP = new PDMLParser(is, app.protocolMap);
     		PacketProcessor pp = new PacketProcessor(client, app.indexName);
    		pdmlP.parse(pp);
    		
    	} catch(Exception io){
    		io.printStackTrace();
    		System.out.println("Error while read: "+io.getMessage());
    	} finally {
    		client.close();
    	}
    }

	private void loadMapping(Client client, String fileldsFileName) throws IOException {
		protocolMap = FieldMapping.loadProtocoMapping( fileldsFileName ); 
		for( String protoName : protocolMap.keySet()) { 
			final IndicesExistsResponse res = client.admin().indices().prepareExists(indexName).execute().actionGet();
			if (res.isExists()) {
			    final DeleteIndexRequestBuilder delIdx = client.admin().indices().prepareDelete(indexName);
			    delIdx.execute().actionGet();
			}
	
			final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);
	
			// MAPPING GOES HERE
			 		
			String mappincDoc = MappingCreator.createMapping(protoName, protocolMap.get(protoName));
			createIndexRequestBuilder.addMapping( protoName, mappincDoc);
			logger.debug(protoName+" mapping described as:"+mappincDoc);
			// MAPPING DONE
			createIndexRequestBuilder.execute().actionGet();
		}
	}
//=================================================================================

	
	
}
