package com.atalas.callaider.elastic.iface;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map.Entry;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.atalas.callaider.elastic.iface.PDMLParser.PacketListener;

public class PacketProcessor implements PacketListener {
	private final Client client;
	private final String indexName;
	private final SimpleDateFormat sdf;
	
	public PacketProcessor( Client client, String indexName){
		this.client = client;	
		this.indexName = indexName;
		this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	}
	
	@Override
	public void processNextPacket(List<ProtocolContainer> packet) {
		
		BulkRequestBuilder brb = new BulkRequestBuilder(client);
		
        try{
	        for( ProtocolContainer protocol: packet){
	        	String docType = protocol.getDescription().getName();
				final IndexRequestBuilder indexRequestBuilder = 
	        			client.prepareIndex(indexName, docType, protocol.getId());
	        	
	        	final XContentBuilder contentBuilder = jsonBuilder().startObject().prettyPrint();
	        	if( null!=protocol.getParentId()){
	        		contentBuilder.field("parentId", protocol.getParentId());
	        	}
	        	for( Entry<String, Object> field :protocol.getFieldsEntrySet()){
		        	contentBuilder.field(field.getKey(), field.getValue());
	        	}
	        	contentBuilder.endObject();
		        indexRequestBuilder.setSource(contentBuilder);
		        
		        Object tsfld = protocol.getField("x_timestamp");
		        if(null!=tsfld) 
		        	indexRequestBuilder.setTimestamp( sdf.format(tsfld));
		        
		        AppXML.logger.debug(docType+"["+protocol.getId()+"] described as:"+contentBuilder.string());
		        brb.add(indexRequestBuilder);
	        }
	        
	        BulkResponse result = brb.execute().actionGet();
    		if( result.hasFailures()){
    			System.out.println( "Result is:" + result.buildFailureMessage());
    			for( BulkItemResponse bir: result.getItems()){
    				if( bir.isFailed() ){
    					System.out.println( "\t" + bir.getFailureMessage());
    				}
    			}
    		}

        } catch (Exception e){
        	e.printStackTrace();
        }
	}
}

