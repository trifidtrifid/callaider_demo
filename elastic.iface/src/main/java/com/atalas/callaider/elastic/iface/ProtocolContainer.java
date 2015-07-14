package com.atalas.callaider.elastic.iface;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.atalas.callaider.elastic.iface.storage.FieldMapping;

public class ProtocolContainer {
	private final Description description;
	private final Map<String,Object> fields;
	private final String id;
	private final String parentId;
	
	public ProtocolContainer(String protocolName, String id, String parentId, Description description) {
		this.description = description;
		fields = new HashMap<>();	
		this.id = id;
		this.parentId = parentId;
	}
	
	public void addField( String name, Object value) throws IOException{
		if( fields.containsKey(name))
			throw new IOException("Protocol:"+description.getName()+" already have field "+name+" with value "+fields.get(name));
		fields.put(name, value);
	}
	public void setField( String name, Object value) throws IOException{
		fields.put(name, value);	
	}
	
	public <T> T getTheField(String key){
		return (T)fields.get(key); 
	}
	
	public Object getField(String name){
		return fields.get(name);
	}

	public String getId() {
		return id;
	}

	public String getParentId() {
		return parentId;
	}
	
	
	public Description getDescription() {
		return description;
	}
	
	public final Set<Entry<String,Object>> getFieldsEntrySet(){
		return fields.entrySet();
	}

	public static class Description {
		private final String name;
		private final int level;
		private final FieldMapping fields;
		
		public Description(String name, int level, FieldMapping fields) {
			this.name = name;
			this.level = level;
			this.fields = fields;
		}

		public String getName() {
			return name;
		}

		public int getLevel() {
			return level;
		}

		public FieldMapping getFields() {
			return fields;
		}
		
	}
}
