package de.tum.i13.client.exceptions.ClientException;

public class ClientException extends Exception{
  
  private String reason;
  private ClientExceptionType type;

  public ClientException(String reason) {
    this.reason = reason;
  }

  public ClientException(String reason, ClientExceptionType type) {
    this.reason = reason;
  }

  public ClientExceptionType getType() {
    return this.type;
  }

  public String getReason() {
    return this.reason;
  }
}
