package de.tum.i13.client.exceptions;

public class ClientException extends Exception{
  
  private String reason;

  public ClientException(String reason) {
    this.reason = reason;
  }

  public String getReason() {
    return this.reason;
  }
}
