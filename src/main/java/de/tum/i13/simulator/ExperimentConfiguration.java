package de.tum.i13.simulator;

import de.tum.i13.server.cache.CachingStrategy;

import static de.tum.i13.server.cache.CachingStrategy.LFU;

public class ExperimentConfiguration {

    public static final int DEFAULT_STARTING_SERVER_COUNT = 1;
    public static final int DEFAULT_STARTING_CLIENT_COUNT = 0;
    public static final int DEFAULT_FINAL_SERVER_COUNT = 5;
    public static final int DEFAULT_FINAL_CLIENT_COUNT = 10;
    public static final int DEFAULT_SERVER_START_DELAY = 90;
    public static final int DEFAULT_CLIENT_START_DELAY = 20;
    public static final int DEFAULT_SERVER_CACHE_SIZE = 500;
    public static final int DEFAULT_BTREE_NODE_SIZE = 100;
    public static final CachingStrategy DEFAULT_SERVER_CACHE_STRATEGY = LFU;
    public static final String DEFAULT_STATS_NAME = "LFU";

    private final int startingServerCount;
    private final int startingClientCount;
    private final int finalServerCount;
    private final int finalClientCount;

    public int getStartingServerCount() {
        return startingServerCount;
    }

    public int getStartingClientCount() {
        return startingClientCount;
    }

    public int getFinalServerCount() {
        return finalServerCount;
    }

    public int getFinalClientCount() {
        return finalClientCount;
    }

    public int getServerStartDelay() {
        return serverStartDelay;
    }

    public int getClientStartDelay() {
        return clientStartDelay;
    }

    public int getServerCacheSize() {
        return serverCacheSize;
    }

    public int getbTreeNodeSize() {
        return bTreeNodeSize;
    }

    public CachingStrategy getServerCachingStrategy() {
        return serverCachingStrategy;
    }

    public String getStatsName() {
        return statsName;
    }

    private final int serverStartDelay;
    private final int clientStartDelay;
    private final int serverCacheSize;
    private final int bTreeNodeSize;
    private final CachingStrategy serverCachingStrategy;
    private final String statsName;

    private ExperimentConfiguration(Builder builder) {
        startingServerCount = builder.startingServerCount;
        startingClientCount = builder.startingClientCount;
        finalServerCount = builder.finalServerCount;
        finalClientCount = builder.finalClientCount;
        serverStartDelay = builder.serverStartDelay;
        clientStartDelay = builder.clientStartDelay;
        serverCacheSize = builder.serverCacheSize;
        bTreeNodeSize = builder.bTreeNodeSize;
        serverCachingStrategy = builder.serverCachingStrategy;
        statsName = builder.statsName;
    }

    public static Builder experimentConfiguration() {
        return new Builder();
    }


    public static final class Builder {

        private int startingServerCount;
        private int startingClientCount;
        private int finalServerCount;
        private int finalClientCount;
        private int serverStartDelay;
        private int clientStartDelay;
        private int serverCacheSize;
        private int bTreeNodeSize;
        private CachingStrategy serverCachingStrategy;
        private String statsName;

        private Builder() {
            startingServerCount = DEFAULT_STARTING_SERVER_COUNT;
            startingClientCount = DEFAULT_STARTING_CLIENT_COUNT;
            finalServerCount = DEFAULT_FINAL_SERVER_COUNT;
            finalClientCount = DEFAULT_FINAL_CLIENT_COUNT;
            serverStartDelay = DEFAULT_SERVER_START_DELAY;
            clientStartDelay = DEFAULT_SERVER_START_DELAY;
            serverCacheSize = DEFAULT_SERVER_CACHE_SIZE;
            bTreeNodeSize = DEFAULT_BTREE_NODE_SIZE;
            serverCachingStrategy = DEFAULT_SERVER_CACHE_STRATEGY;
            statsName = DEFAULT_STATS_NAME;
        }

        public Builder startingServerCount(int startingServerCount) {
            this.startingServerCount = startingServerCount;
            return this;
        }

        public Builder startingClientCount(int startingClientCount) {
            this.startingClientCount = startingClientCount;
            return this;
        }

        public Builder finalServerCount(int finalServerCount) {
            this.finalServerCount = finalServerCount;
            return this;
        }

        public Builder finalClientCount(int finalClientCount) {
            this.finalClientCount = finalClientCount;
            return this;
        }

        public Builder serverStartDelay(int serverStartDelay) {
            this.serverStartDelay = serverStartDelay;
            return this;
        }

        public Builder clientStartDelay(int clientStartDelay) {
            this.clientStartDelay = clientStartDelay;
            return this;
        }

        public Builder serverCacheSize(int serverCacheSize) {
            this.serverCacheSize = serverCacheSize;
            return this;
        }

        public Builder bTreeNodeSize(int bTreeNodeSize) {
            this.bTreeNodeSize = bTreeNodeSize;
            return this;
        }

        public Builder serverCachingStrategy(CachingStrategy serverCachingStrategy) {
            this.serverCachingStrategy = serverCachingStrategy;
            return this;
        }

        public Builder statsName(String statsName) {
            this.statsName = statsName;
            return this;
        }

        public ExperimentConfiguration build() {
            return new ExperimentConfiguration(this);
        }

    }

}
