package de.tum.i13.simulator;

public class ClientStats {
  public double putTime = 0;
  public double getTime = 0;
  public double deleteTime = 0;
  public int putCount = 0;
  public int getCount = 0;
  public int deleteCount = 0;
  public int putFailCount = 0;
  public int getFailCount = 0;
  public int deleteFailCount = 0;

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
    System.out.println(">> STATE");
    System.out.println(String.format("GET(%d, %d, %f)", getCount, getFailCount, getTime));
    System.out.println(String.format("PUT(%d, %d, %f)", putCount, putFailCount, putTime));
    System.out.println(String.format("DELETE(%d, %d, %f)", deleteCount, deleteFailCount, deleteTime));
  }

  public void add(ClientStats c) {
    this.putTime = c.putTime;
    this.getTime = c.getTime;
    this.deleteTime = c.deleteTime;
    this.putCount = c.putCount;
    this.getCount = c.getCount;
    this.deleteCount = c.deleteCount;
    this.putFailCount = c.putFailCount;
    this.getFailCount = c.getFailCount;
    this.deleteFailCount = c.deleteFailCount;
  }
}
