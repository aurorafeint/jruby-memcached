package com.openfeint.memcached;

import com.openfeint.memcached.error.Error;
import com.openfeint.memcached.transcoder.MarshalTranscoder;
import com.openfeint.memcached.transcoder.MarshalZlibTranscoder;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Locator;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;
import org.jruby.Ruby;
import org.jruby.runtime.ThreadContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

class ConnectionPool {
    private int poolSize;
    private int currentIndex;
    private MemcachedClient[] connections;

    public ConnectionPool(ThreadContext context, List<String> servers, Map<String, String> options, int poolSize) {
        this.poolSize = poolSize;
        currentIndex = 0;
        initConnections(context, servers, options);
    }

    public MemcachedClient getConnection() {
        currentIndex = currentIndex++ % poolSize;
        return connections[currentIndex];
    }

    public void shutdown() {
        for (MemcachedClient connection : connections) {
            connection.shutdown();
        }
    }

    private void initConnections(ThreadContext context, List<String> servers, Map<String, String> options) {
        connections = new MemcachedClient[poolSize];
        for (int i = 0; i < poolSize; i++) {
            connections[i] = initConnection(context, servers, options);
        }
    }

    private MemcachedClient initConnection(ThreadContext context, List<String> servers, Map<String, String> options) {
        Ruby ruby = context.getRuntime();
        List<InetSocketAddress> addresses = AddrUtil.getAddresses(servers);
        try {
            ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();

            String distributionValue = "ketama";
            String hashValue = "fnv1_32";
            boolean binaryValue = false;
            boolean shouldOptimize = false;
            String transcoderValue = null;
            if (!options.isEmpty()) {
                if (options.containsKey("distribution")) {
                    distributionValue = options.get("distribution");
                }
                if (options.containsKey("hash")) {
                    hashValue = options.get("hash");
                }
                if (options.containsKey("binary_protocol")) {
                    binaryValue = Boolean.parseBoolean(options.get("binary_protocol"));
                }
                if (options.containsKey("should_optimize")) {
                    shouldOptimize = Boolean.parseBoolean(options.get("should_optimize"));
                }
                if (options.containsKey("transcoder")) {
                    transcoderValue = options.get("transcoder");
                }
            }

            if ("array_mod".equals(distributionValue)) {
                builder.setLocatorType(Locator.ARRAY_MOD);
            } else if ("ketama".equals(distributionValue) || "consistent_ketama".equals(distributionValue)) {
                builder.setLocatorType(Locator.CONSISTENT);
            } else {
                throw Error.newNotSupport(ruby, "distribution not support");
            }
            if ("native".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.NATIVE_HASH);
            } else if ("crc".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.CRC_HASH);
            } else if ("fnv1_64".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.FNV1_64_HASH);
            } else if ("fnv1a_64".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH);
            } else if ("fnv1_32".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.FNV1_32_HASH);
            } else if ("fnv1a_32".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.FNV1A_32_HASH);
            } else if ("ketama".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.KETAMA_HASH);
            } else {
                throw Error.newNotSupport(ruby, "hash not support");
            }

            if (binaryValue) {
                builder.setProtocol(Protocol.BINARY);
            }

            if (shouldOptimize) {
                builder.setShouldOptimize(true);
            }

            if (options.containsKey("timeout")) {
                int timeout = Integer.parseInt(options.get("timeout"));
                builder.setOpTimeout(timeout);
            }

            builder.setDaemon(true);

            Transcoder transcoder;
            if ("marshal_zlib".equals(transcoderValue)) {
                transcoder = new MarshalZlibTranscoder(ruby);
            } else {
                transcoder = new MarshalTranscoder(ruby);
            }
            builder.setTranscoder(transcoder);

            return new MemcachedClient(builder.build(), addresses);
        } catch (IOException e) {
            throw ruby.newIOErrorFromException(e);
        }
    }
}
