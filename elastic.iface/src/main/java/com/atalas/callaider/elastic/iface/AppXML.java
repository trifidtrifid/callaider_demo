package com.atalas.callaider.elastic.iface;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.atalas.callaider.elastic.iface.storage.FieldMapping;
import com.atalas.callaider.elastic.iface.storage.MappingCreator;
import com.atalas.callaider.elastic.iface.storage.StorageInterface;
import com.atalas.callaider.flow.mcid.LBSLogRecord;
import com.atalas.callaider.flow.mcid.McidFlow;
import com.atalas.callaider.flow.mcid.McidPacketProcessor;

public class AppXML {
	
	static Logger logger = Logger.getLogger(AppXML.class.getName()) ;
	
	public static final String indexName = "mlbs";
	public final String documentType = "packet";
	private Map<String,ProtocolContainer.Description>  protocolMap;
	
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
    	
    	Client client = StorageInterface.createClient();

		try{			
			
			createIndexes(args[1], app, client);
			PacketProcessor pp = new McidPacketProcessor(client, app.indexName);
			InputStream is = args[0].equals("-") ? System.in : new FileInputStream(args[0]);
			PDMLParser pdmlP = new PDMLParser(is, app.protocolMap);    		
     		pdmlP.parse(pp);
    		
    	} catch(Exception io){
    		io.printStackTrace();
    		System.out.println("Error while read: "+io.getMessage());
    	} finally {
    		client.close();
    	}
    }

	private static void createIndexes(String configurationFileName, AppXML app, Client client)
			throws IOException {
		
		StorageInterface si = new StorageInterface(client, app.indexName);
		si.deleteIndex(app.indexName);
		
		final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(app.indexName);		
		app.loadMapping(createIndexRequestBuilder, configurationFileName);
		createIndexRequestBuilder.addMapping(McidFlow.class.getSimpleName(), si.createObjectMapping(McidFlow.class));		
		createIndexRequestBuilder.addMapping(LBSLogRecord.class.getSimpleName(), si.createObjectMapping(LBSLogRecord.class));
		
		createIndexRequestBuilder.execute().actionGet();
	}

	private void loadMapping(CreateIndexRequestBuilder createIndexRequestBuilder, String fileldsFileName) throws IOException {
		
		protocolMap = FieldMapping.loadProtocoMapping( fileldsFileName ); 
		for( String protoName : protocolMap.keySet()) { 			
			String mappincDoc = MappingCreator.createMapping(protoName, protocolMap.get(protoName));
			createIndexRequestBuilder.addMapping( protoName, mappincDoc);
			logger.debug(protoName+" mapping described as:"+mappincDoc); 
		}		
	}
//=================================================================================

	
	
}
