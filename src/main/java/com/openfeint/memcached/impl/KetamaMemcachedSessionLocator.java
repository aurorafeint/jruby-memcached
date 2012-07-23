package com.openfeint.memcached.impl;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import net.rubyeye.xmemcached.HashAlgorithm;
import net.rubyeye.xmemcached.impl.AbstractMemcachedSessionLocator;
import net.rubyeye.xmemcached.impl.MemcachedTCPSession;
import net.rubyeye.xmemcached.networking.MemcachedSession;

import com.google.code.yanf4j.core.Session;

/**
 * This is grabbed from xmemcached.
 * what I changed are as follows:
 * 1. remove cwNginxUpstreamConsistent
 * 2. sockStr should be hostAddress (port is 11211) or hostAddress:port (port is not 11211)
 * 3. hash algorithm only used for generating hash
 */
public class KetamaMemcachedSessionLocator extends
		AbstractMemcachedSessionLocator {

	static final int NUM_REPS = 160;
	private transient volatile TreeMap<Long, List<Session>> ketamaSessions = new TreeMap<Long, List<Session>>();
	private final HashAlgorithm hashAlg;
	private volatile int maxTries;
	private final Random random = new Random();

	static final int DEFAULT_PORT = 11211;

	public KetamaMemcachedSessionLocator() {
		this.hashAlg = HashAlgorithm.KETAMA_HASH;
	}

	public KetamaMemcachedSessionLocator(HashAlgorithm alg) {
		this.hashAlg = alg;
	}

	public KetamaMemcachedSessionLocator(List<Session> list, HashAlgorithm alg) {
		super();
		this.hashAlg = alg;
		this.buildMap(list, alg);
	}

	private final void buildMap(Collection<Session> list, HashAlgorithm alg) {
		TreeMap<Long, List<Session>> sessionMap = new TreeMap<Long, List<Session>>();

		String sockStr;
		for (Session session : list) {
      InetSocketAddress serverAddress = session.getRemoteSocketAddress();
      if (serverAddress.getPort() == DEFAULT_PORT) {
				sockStr = serverAddress.getAddress().getHostAddress();
      } else {
				sockStr = serverAddress.getAddress().getHostAddress() + ":" + serverAddress.getPort();
      }
			/**
			 * Duplicate 160 X weight references
			 */
			int numReps = NUM_REPS;
			if (session instanceof MemcachedTCPSession) {
				numReps *= ((MemcachedSession) session).getWeight();
			}
      for (int i = 0; i < numReps / 4; i++) {
        byte[] digest = HashAlgorithm.computeMd5(sockStr + "-" + i);
        for (int h = 0; h < 4; h++) {
          long k = (long) (digest[3 + h * 4] & 0xFF) << 24
              | (long) (digest[2 + h * 4] & 0xFF) << 16
              | (long) (digest[1 + h * 4] & 0xFF) << 8
              | digest[h * 4] & 0xFF;
          this.getSessionList(sessionMap, k).add(session);
        }

      }
		}
		this.ketamaSessions = sessionMap;
		this.maxTries = list.size();
	}

	private List<Session> getSessionList(
			TreeMap<Long, List<Session>> sessionMap, long k) {
		List<Session> sessionList = sessionMap.get(k);
		if (sessionList == null) {
			sessionList = new ArrayList<Session>();
			sessionMap.put(k, sessionList);
		}
		return sessionList;
	}

	public final Session getSessionByKey(final String key) {
		if (this.ketamaSessions == null || this.ketamaSessions.size() == 0) {
			return null;
		}
		long hash = this.hashAlg.hash(key);
		Session rv = this.getSessionByHash(hash);
		int tries = 0;
		while (!this.failureMode && (rv == null || rv.isClosed())
				&& tries++ < this.maxTries) {
			hash = this.nextHash(hash, key, tries);
			rv = this.getSessionByHash(hash);
		}
		return rv;
	}

	public final Session getSessionByHash(final long hash) {
		TreeMap<Long, List<Session>> sessionMap = this.ketamaSessions;
		if (sessionMap.size() == 0) {
			return null;
		}
		Long resultHash = hash;
		if (!sessionMap.containsKey(hash)) {
			// Java 1.6 adds a ceilingKey method, but xmemcached is compatible
			// with jdk5,So use tailMap method to do this.
			SortedMap<Long, List<Session>> tailMap = sessionMap.tailMap(hash);
			if (tailMap.isEmpty()) {
				resultHash = sessionMap.firstKey();
			} else {
				resultHash = tailMap.firstKey();
			}
		}
		//
		// if (!sessionMap.containsKey(resultHash)) {
		// resultHash = sessionMap.ceilingKey(resultHash);
		// if (resultHash == null && sessionMap.size() > 0) {
		// resultHash = sessionMap.firstKey();
		// }
		// }
		List<Session> sessionList = sessionMap.get(resultHash);
		if (sessionList == null || sessionList.size() == 0) {
			return null;
		}
		int size = sessionList.size();
		return sessionList.get(this.random.nextInt(size));
	}

	public final long nextHash(long hashVal, String key, int tries) {
		long tmpKey = this.hashAlg.hash(tries + key);
		hashVal += (int) (tmpKey ^ tmpKey >>> 32);
		hashVal &= 0xffffffffL; /* truncate to 32-bits */
		return hashVal;
	}

	public final void updateSessions(final Collection<Session> list) {
		this.buildMap(list, this.hashAlg);
	}
}
