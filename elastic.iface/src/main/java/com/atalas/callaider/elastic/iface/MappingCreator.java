package com.atalas.callaider.elastic.iface;

import java.util.Map.Entry;

import com.atalas.callaider.elastic.iface.PDMLParser.FieldMapping;
import com.atalas.callaider.elastic.iface.PDMLParser.FieldMapping.Type;

public class MappingCreator {

	public static String createMapping( String typeName, FieldMapping mapping){
		String mappingString = "{\"" + typeName + "\" : { \"properties\" : {";
		mappingString = processTheFIeldMap(mapping, mappingString);
		if( mappingString.endsWith(","))
			mappingString = mappingString.substring(0,mappingString.length()-1);
		mappingString += "}}}";
		return mappingString;
	}

	private static String processTheFIeldMap(FieldMapping mapping,
			String mappingString) {
		for( Entry<String,FieldMapping> fme : mapping.lowerLevelMap.entrySet() ){
			FieldMapping value = fme.getValue();
			if( value.type == Type.CONTAINER && null!=value.lowerLevelMap && !value.lowerLevelMap.isEmpty()){
				mappingString = processTheFIeldMap(value, mappingString);
			} else {
				mappingString += "\""+fme.getKey()+"\":{ \"type\": \"";
				switch( value.type){
				case INT:	
					mappingString += "integer\"";
					break;
				case DATE:
					mappingString += "date\"";
					break;
				case FLOAT:					
					mappingString += "float\"";
					break;
				default: 				
					mappingString += "string\"";
					break;
				}
				mappingString += "},";
			}
						
		}
		return mappingString;
	}

}
