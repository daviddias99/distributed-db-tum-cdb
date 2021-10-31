package de.tum.i13.client.exceptions.ClientException;

public class ClientException extends Exception{
  
  private String reason;
  private ClientExceptionType type;

  public ClientException(String reason) {
    this.reason = reason;
    this.type = ClientExceptionType.UNKNOWN_ERROR;
  }

  public ClientException(String reason, ClientExceptionType type) {
    this.reason = reason;
    this.type = type;
  }

  public ClientExceptionType getType() {
    return this.type;
  }

  public String getReason() {
    return this.reason;
  }
}
