package com.atalas.callaider.elastic.iface.storage;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.SearchHit;

import com.atalas.callaider.elastic.iface.ProtocolContainer;
import com.atalas.callaider.elastic.iface.ProtocolContainer.Description;
import com.google.gson.Gson;


public class StorageInterface {
	
	private final Client client;
	private final Gson gson;
	private final String index;

	public StorageInterface(Client client, Map<String, Description> protoMap, String index) {

		this.client = client;		
		this.gson = new Gson();
		this.index = index;
	}
	
	public <T> List<T> searchObjects( Class<T> clazz, Map<String, Object> fields ){
		
		List<T> rslt = new ArrayList<T>();		
		SearchRequestBuilder searchReq = client.prepareSearch( index ).setTypes(clazz.getSimpleName());
		for( Entry<String,Object> fldEntry : fields.entrySet() )
			searchReq.setPostFilter(FilterBuilders.termFilter(fldEntry.getKey(), fldEntry.getValue()));
			
		SearchResponse sr = searchReq.execute().actionGet();		
		SearchHit[] results = sr.getHits().getHits();
        for(SearchHit hit : results){

            String sourceAsString = hit.getSourceAsString();
            if (sourceAsString != null) {
            	T nextObj = gson.fromJson( sourceAsString, clazz);            	
            	setId(nextObj, hit.getId());
				rslt.add(nextObj);                
            }
        }        
		return rslt;
	}
	
	//================================================================================================
	
	public <T> String saveObject( T object ){
		Class<? extends Object> oclz = object.getClass();
		String id = getId(object, oclz);
		String simpleName = oclz.getSimpleName();
		IndexRequestBuilder indexReq = client.prepareIndex(index, simpleName, id);
		indexReq.setSource( gson.toJson(object) );
		IndexResponse response = indexReq.execute()
		        .actionGet();
		return response.getId();
	}
	

	private <T> String getId(T object, Class<? extends Object> oclz) {
		String id = null;
		try {
			Field idFld = oclz.getField("_id");
			if( idFld.isAccessible() )
				id = ""+idFld.get( object );
			else {			
				Method getIdMthd = oclz.getMethod("get_Id", new Class[]{});
				if( getIdMthd.isAccessible()){
					id = ""+getIdMthd.invoke(object, new Object[]{});
				}
			}
		} catch (Exception e){}
		return id;
	}
	
	private <T> void setId(T object, String id) {
		/*if( null!=id){
			try {
				Field idFld = object.getClass().getField("id");
				if( idFld.isAccessible() )
					idFld.set(object, id);
				else {			
					Method setIdMthd;
					try {
						setIdMthd = object.getClass().getMethod("setId", new Class[]{String.class});
					} catch (Exception e) {						
						e.printStackTrace();
					}
					if( setIdMthd.isAccessible()){
						setIdMthd.invoke(object, new Object[]{id});
					}
				}
			} catch (Exception e){}
		}*/
	}
	
	public <T> String updateObject( T object ){
		return saveObject(object);		
	}
}
