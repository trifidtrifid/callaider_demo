package com.atalas.callaider.elastic.iface;

import java.math.BigInteger;
import java.security.SecureRandom;

public final class IdGenerator {
	private SecureRandom random = new SecureRandom();

	public String nextId() {
		return new BigInteger(130, random).toString(32);
	}
}