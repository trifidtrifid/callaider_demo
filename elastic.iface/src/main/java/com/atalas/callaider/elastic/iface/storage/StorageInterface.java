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
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

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

		List<T> rslt = new ArrayList<T>();
		SearchRequestBuilder searchReq = client.prepareSearch(index).setTypes(
				clazz.getSimpleName());
		for (Entry<String, Object> fldEntry : keyValues.entrySet()) {
			Object value = fldEntry.getValue();
			if( value.getClass().isArray() && ((Object[])value).length == 2 ){ //Range 
				searchReq.setPostFilter(FilterBuilders.rangeFilter(
						fldEntry.getKey()).from(((Object[])value)[0]).to(((Object[])value)[1]));
			} else {
				searchReq.setPostFilter(FilterBuilders.termFilter(
						fldEntry.getKey(), value));
			}
		}
		
		if( null!=sortField){
			searchReq.addSort(sortField, null==sortDesc || !sortDesc ? SortOrder.ASC : SortOrder.DESC);
		}
		SearchResponse sr = searchReq.execute().actionGet();
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
	// ================================================================================================
	public <T> T searchSingleObjects(Class<T> clazz, Map<String, Object> fields, String sortField, Boolean sortDesc) {

		List<T> foundObjects = searchObjects(clazz, fields,sortField,sortDesc);
		if(foundObjects.size() == 0){
			logger.warn("No object "+clazz.getName()+" found by filter '"+filterAsString(fields)+"'");
			return null;
		} else if(foundObjects.size() > 1){
			logger.warn("There are "+ foundObjects.size() +" "+clazz.getName()+" found by filter '"+filterAsString(fields)+"'");
		}	
		return foundObjects.get(0);
	}
	
	
	public <T> T searchSingleObjects(Class<T> clazz, Map<String, Object> fields) {
		return searchSingleObjects(clazz, fields, null, null);
	}

	
	private String filterAsString(Map<String,Object> filter){
		String filterStr = "";
		for (Entry<String, Object> fldEntry : filter.entrySet())
			filterStr += fldEntry.getKey()+"=" + fldEntry.getValue()+" ";
		return filterStr;
	}

	// ================================================================================================

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
		String mappingString = "{ \"" + className
				+ "\" : { \"properties\" : { \"_id\" : {\"type\" : \"string\", \"store\" : true, \"copy_to\": \"id\" }, ";

		mappingString += createFieldsIndex(objCls);

		if (mappingString.endsWith(","))
			mappingString = mappingString.substring(0,
					mappingString.length() - 1);
		mappingString += "}}}";

		// DELETE OLD INDEX
		final IndicesExistsResponse res = client.admin().indices()
				.prepareExists(index).execute().actionGet();
		if (res.isExists()) {
			final DeleteIndexRequestBuilder delIdx = client.admin().indices()
					.prepareDelete(index);
			delIdx.execute().actionGet();
		}

		final CreateIndexRequestBuilder createIndexRequestBuilder = client
				.admin().indices().prepareCreate(index);

		// MAPPING GOES HERE

		createIndexRequestBuilder.addMapping(className, mappingString);
		// logger.debug(className+" mapping described as:"+mappingString);
		// MAPPING DONE
		ListenableActionFuture<CreateIndexResponse> execute = createIndexRequestBuilder
				.execute();
		execute.actionGet();

		return mappingString;

	}

	private String createFieldsIndex(Class objCls) {
		
		String mappingString = "";
		for (Field fld : objCls.getFields()) {
			Type fclass = fld.getType();
			String fldName = fld.getName();			

			if (fclass.equals(String.class)) {
				mappingString += " \"" + fldName
						+ "\": { \"index\" : \"not_analyzed\", \"type\":\"string\" },";
			} else if (fclass.equals(Integer.class)
					|| fclass.equals(Long.class)) {
				mappingString += " \"" + fldName + "\": { \"index\" : \"not_analyzed\", \"type\":\"long\"},";
			} else if (fclass.equals(Double.class)
					|| fclass.equals(Float.class)) {
				mappingString += " \"" + fldName
						+ "\": { \"index\" : \"not_analyzed\", \"type\":\"double\"},";
			} else if (fclass.equals(Boolean.class)) {
				mappingString += " \"" + fldName
						+ "\": { \"index\" : \"not_analyzed\", \"type\":\"boolean\"},";

			} else if (fclass.equals(Date.class)) {
				mappingString += " \"" 
						+ fldName 
						+ "\": { \"index\" : \"not_analyzed\", \"type\":\"date\"},";

			} else if (!fld.getType().isArray()) {
				mappingString += "\"" + fldName							 
						+ "\": { \"type\":\"object\", \"index\" : \"not_analyzed\", \"properties\" : {";						
				
				if( fclass.equals(Map.class) ){
					mappingString += "\"tid\" : { \"type\":\"string\"}" ;
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
