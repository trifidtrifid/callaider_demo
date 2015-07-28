package com.atalas.callaider.elastic.iface.storage;

import java.util.Date;

public abstract class StorageIndexedInterface {
	public static class Location {
		public Location(Double longitude, Double lattitude) {
			this.lon = longitude;
			this.lat = lattitude;
		}
		Double lon;
		Double lat;
	}
	public String id = null;	
	public Date x_timestamp = null;
}
