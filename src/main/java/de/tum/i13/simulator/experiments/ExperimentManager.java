package de.tum.i13.simulator.experiments;

import de.tum.i13.simulator.client.ClientManager;
import de.tum.i13.simulator.events.StatsAccumulator;
import de.tum.i13.simulator.server.ServerManager;

public class ExperimentManager {

    private ServerManager serverManager;
    private StatsAccumulator statsAccumulator;
    private ClientManager clientManager;

    public ServerManager getServerManager() {
        return serverManager;
    }

    public void setServerManager(ServerManager serverManager) {
        this.serverManager = serverManager;
    }

    public StatsAccumulator getStatsAccumulator() {
        return statsAccumulator;
    }

    public void setStatsAccumulator(StatsAccumulator statsAccumulator) {
        this.statsAccumulator = statsAccumulator;
    }

    public ClientManager getClientManager() {
        return clientManager;
    }
    public void setClientManager(ClientManager clientManager) {
        this.clientManager = clientManager;
    }

}
