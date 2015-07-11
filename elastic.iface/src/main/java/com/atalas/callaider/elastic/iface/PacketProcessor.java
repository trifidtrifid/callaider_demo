package com.atalas.callaider.elastic.iface;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.atalas.callaider.elastic.iface.PDMLParser.PacketListener;

public class PacketProcessor implements PacketListener {
	private static Logger logger = Logger.getLogger(PacketProcessor.class.getName()) ;

	public PacketProcessor(Client client){
		m_client = client;			
	}
	
	@Override
	public void processNextPacket(Map<String, Object> packet) {
		final String documentType = AppXML.documentType;
        final IndexRequestBuilder indexRequestBuilder = m_client.prepareIndex(AppXML.indexName, documentType, null);

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
	
	private Client m_client;
};
