package com.atalas.callaider.elastic.iface.storage;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.atalas.callaider.elastic.iface.storage.StorageIndexedInterface.Location;
import com.atalas.callaider.flow.mcid.McidFlow;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class StorageInterface {

	// private static Logger logger = Logger.getLogger(StorageInterface.class);

	private final Client client;
	private final Gson gson;
	private final String index;
	private final IdGenerator idGen;
	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

	public StorageInterface(Client client, String index) {

		this.client = client;
		this.gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").create();
		this.index = index;
		this.idGen = new IdGenerator();
		 sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");		 
	}
	
	public final Client getClient(){return client;}
	
	public StorageInterface(String index) {
		this( StorageInterface.createClient(), index);
	}


	public static Client createClient() {
        final ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
        TransportClient transportClient = new TransportClient(settings)
        	.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        return transportClient;
    }

	public <T> List<T> searchObjects(Class<T> clazz, Map<String, Object> keyValues) {
		return searchObjects(clazz, keyValues, null, null);
	}

	public <T> List<T> searchObjects(Class<T> clazz, Map<String, Object> keyValues, String sortField, Boolean sortDesc) {

		SearchRequestBuilder searchReq = prepareSearchRequest(clazz, keyValues,
				sortField, sortDesc);
		
		SearchResponse sr = searchReq.execute().actionGet();
		return loadSearchResults(clazz, keyValues, sr);
		
	}

	private <T> List<T> loadSearchResults(Class<T> clazz,
			Map<String, Object> keyValues, SearchResponse sr) {
		List<T> rslt = new ArrayList<>();
		SearchHit[] results = sr.getHits().getHits();
		for (SearchHit hit : results) {

			String sourceAsString = hit.getSourceAsString();
			if (sourceAsString != null) {
				logger.debug("Search by '"+filterAsString(keyValues)+"' got object: "+sourceAsString);
				T nextObj = gson.fromJson(sourceAsString, clazz);
				setId(nextObj, hit.getId());
				rslt.add(nextObj);
			}
		} 
		if( results.length == 0){
			logger.warn("Search by '"+filterAsString(keyValues)+"' got NOTHING");
		}
		return rslt;
	}

	private <T> SearchRequestBuilder prepareSearchRequest(Class<T> clazz,
			Map<String, Object> keyValues, String sortField, Boolean sortDesc) {
		SearchRequestBuilder searchReq = client.prepareSearch(index).setTypes(
				clazz.getSimpleName());
		
		if(keyValues.size()>0){
			AndFilterBuilder andFilter = FilterBuilders.andFilter();
			
			for (Entry<String, Object> fldEntry : keyValues.entrySet()) {
				FilterBuilder filterBuilder;
				filterBuilder = createFilter(fldEntry);
				andFilter.add( filterBuilder);
			}
			searchReq.setPostFilter( andFilter );				
		}
		
		if( null!=sortField){
			searchReq.addSort(sortField, null==sortDesc || !sortDesc ? SortOrder.ASC : SortOrder.DESC);
		}
		return searchReq;
	}

	private FilterBuilder createFilter(Entry<String, Object> fldEntry) {
		FilterBuilder filterBuilder;
		Object value = fldEntry.getValue();
		
		if( value.getClass().isArray() && ((Object[])value).length == 2 ){ //Range 
			filterBuilder = FilterBuilders. rangeFilter( 
					fldEntry.getKey()).from(((Object[])value)[0]).to(((Object[])value)[1]);
			
		} else {
			filterBuilder = FilterBuilders.termFilter(
					fldEntry.getKey(), value);
		}
		return filterBuilder;
	}
	// ================================================================================================
	public <T> T searchSingleObjects(Class<T> clazz, Map<String, Object> fields, String sortField, Boolean sortDesc) {

		SearchRequestBuilder searchReq = prepareSearchRequest(clazz, fields, sortField, sortDesc);
		searchReq.setSize(1);
		SearchResponse sr = searchReq.execute().actionGet();
		List<T> foundObjects = loadSearchResults(clazz, fields, sr);
		
		if(foundObjects.size() == 0){
			logger.warn("No object "+clazz.getName()+" found by filter '"+filterAsString(fields)+"'");
			return null;
		} 
		return foundObjects.get(0);
	}
	
	
	public <T> T searchSingleObjects(Class<T> clazz, Map<String, Object> fields) {
		return searchSingleObjects(clazz, fields, null, null);
	}

	
	private String filterAsString(Map<String,Object> filter){
		String filterStr = "";
		for (Entry<String, Object> fldEntry : filter.entrySet()) {
			Object value = fldEntry.getValue();
			filterStr += fldEntry.getKey()+":";
			if( value.getClass().isArray()){
				filterStr += "[";
				for( Object nfv : (Object[])value)
					filterStr += printObjValue(nfv) + ", ";
				filterStr += "] ";
			
			} else {
				filterStr += printObjValue(value)+" ";
			}
		}
		return filterStr;
	}

	// ================================================================================================

	private final SimpleDateFormat sdf2;

	private String printObjValue(Object value) {
		if( value instanceof Date ){
			return sdf2.format((Date)value);
		}
		return ""+value;
	}

	public <T extends StorageIndexedInterface> String saveObject(T object) {

		Class<? extends Object> oclz = object.getClass();
		String id = getId(object, oclz);
		String simpleName = oclz.getSimpleName();
		if (null == id) {
			id = idGen.nextId();
			setId(object, id);
		}
		IndexRequestBuilder indexReq = client.prepareIndex(index, simpleName,id);
		String json = gson.toJson(object);
		logger.debug("Saved object '"+json+"'");
		indexReq.setSource(json);
		if( null!=object.x_timestamp) {
			indexReq.setTimestamp( sdf.format(object.x_timestamp));	
			indexReq.setId(id);
		}
		IndexResponse response = indexReq.execute().actionGet();
		return response.getId();
	}

	private <T> String getId(T object, Class<? extends Object> oclz) {

		if (object instanceof StorageIndexedInterface) {
			return ((StorageIndexedInterface) object).id;
		}
		return null;
	}

	private <T> void setId(T object, String id) {
		if (object instanceof StorageIndexedInterface) {
			((StorageIndexedInterface) object).id = id;
		}
	}

	public <T extends StorageIndexedInterface> String updateObject(T object) {
		return saveObject(object);
	}

	public String createObjectMapping(Class objCls) {

		String className = objCls.getSimpleName();
		String mappingString = "{\"" + className + "\" : { _id: { \"path\" : \"id\" }, \"properties\" : {";
				 
		mappingString += createFieldsIndex(objCls);

		if (mappingString.endsWith(","))
			mappingString = mappingString.substring(0,
					mappingString.length() - 1);
		mappingString += "}}}";
		return mappingString;

	}

	public void deleteIndex( String index ) {
		// DELETE INDEX if exists
		final IndicesExistsResponse res = client.admin().indices()
				.prepareExists(index).execute().actionGet();
		if (res.isExists()) {
			final DeleteIndexRequestBuilder delIdx = client.admin().indices()
					.prepareDelete(index);
			delIdx.execute().actionGet();
		}
	}

	private String createFieldsIndex(Class objCls) {
		
		String mappingString = "";
		for (Field fld : objCls.getFields()) {
			Type fclass = fld.getType();
			String fldName = fld.getName();			

			if (fclass.equals(String.class)) {
				mappingString += " \"" + fldName
						+ "\": { \"index\" : \"not_analyzed\", \"type\" : \"string\" },";
			} else if (fclass.equals(Integer.class)
					|| fclass.equals(Long.class)) {
				mappingString += " \"" + fldName + "\" : { \"type\":\"long\"},";
			} else if (fclass.equals(Double.class)
					|| fclass.equals(Float.class)) {
				mappingString += " \"" + fldName
						+ "\": { \"type\" : \"double\"},";
			} else if (fclass.equals(Boolean.class)) {
				mappingString += " \"" + fldName
						+ "\": { \"type\" : \"boolean\"},";

			} else if (fclass.equals(Date.class)) {
				mappingString += " \"" 
						+ fldName 
						+ "\": { \"type\" : \"date\"},";

			} else if (fclass.equals(Location.class)) {
				mappingString += " \"" 
						+ fldName 
						+ "\": {\"type\":\"geo_point\"},";

			} else if (!fld.getType().isArray()) {
				mappingString += "\"" + fldName							 
						+ "\": { \"type\":\"object\", \"properties\" : {";						
				
				if( fclass.equals(Map.class) ){
					mappingString += "\"tid\" : { \"type\" : \"string\"}" ;
				} else {
					mappingString += createFieldsIndex(fld.getType());
				}				
				mappingString += "}},";
			}
		}
		if( mappingString.endsWith(","))
			mappingString = mappingString.substring(0,mappingString.length()-1);
		return mappingString;
	}
	static Logger logger = Logger.getLogger(StorageInterface.class);
	
}
