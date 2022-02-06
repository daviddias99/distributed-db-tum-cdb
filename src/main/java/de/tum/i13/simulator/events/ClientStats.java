package de.tum.i13.simulator.events;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClientStats implements TimeEvent {

    private static final Logger LOGGER = LogManager.getLogger(ClientStats.class);

    public double putTime = 0;
    public double getTime = 0;
    public double deleteTime = 0;
    public int putCount = 0;
    public int getCount = 0;
    public int deleteCount = 0;
    public int putFailCount = 0;
    public int getFailCount = 0;
    public int deleteFailCount = 0;
    public DelayedEvent periodEvent;
    public int timeStep = -1;

    public ClientStats() {
        this.reset();
    }

    public void put(double time, boolean fail) {
        this.putTime += time;
        this.putCount++;
        this.putFailCount += fail ? 1 : 0;
    }

    public void get(double time, boolean fail) {
        this.getTime += time;
        this.getCount++;
        this.getFailCount += fail ? 1 : 0;
    }

    public void delete(double time, boolean fail) {
        this.deleteTime += time;
        this.deleteCount++;
        this.deleteFailCount += fail ? 1 : 0;
    }

    public void reset() {
        this.putTime = 0;
        this.getTime = 0;
        this.deleteTime = 0;
        this.putCount = 0;
        this.getCount = 0;
        this.deleteCount = 0;
        this.putFailCount = 0;
        this.getFailCount = 0;
        this.deleteFailCount = 0;
    }

    public void print() {
        LOGGER.info(
                "Client stats state\n    GET({}, {} {})\n    PUT({}, {}, {})\n    DEL({}, {}, {})",
                getCount, getFailCount, getTime,
                putCount, putFailCount, putTime,
                deleteCount, deleteFailCount, deleteTime
        );
    }

    public void add(ClientStats c) {
        this.putTime += c.putTime;
        this.getTime += c.getTime;
        this.deleteTime += c.deleteTime;
        this.putCount += c.putCount;
        this.getCount += c.getCount;
        this.deleteCount += c.deleteCount;
        this.putFailCount += c.putFailCount;
        this.getFailCount += c.getFailCount;
        this.deleteFailCount += c.deleteFailCount;
    }

    public String toCSVString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.timeStep).append(",");
        builder.append(this.getCount).append(",");
        builder.append(this.getFailCount).append(",");
        builder.append(this.getTime).append(",");
        builder.append(this.putCount).append(",");
        builder.append(this.putFailCount).append(",");
        builder.append(this.putTime).append(",");
        builder.append(this.deleteCount).append(",");
        builder.append(this.deleteFailCount).append(",");
        builder.append(this.deleteTime).append(",");
        builder.append(putCount - putFailCount + getCount - getFailCount + deleteCount - deleteFailCount).append(",");
        builder.append(this.periodEvent == null ? "" : this.periodEvent.getType()).append("\n");

        return builder.toString();
    }

}
