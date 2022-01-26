package de.tum.i13.shared.net;

import java.util.Objects;

public class NullNetworkLocation implements NetworkLocation {

  @Override
  public String getAddress() {
    return "null";
  }

  @Override
  public int getPort() {
    return 0;
  } 

  @Override
  public boolean equals(Object otherObject) {
      if (this == otherObject) return true;
      if (!(otherObject instanceof NetworkLocation)) return false;
      NetworkLocation that = (NetworkLocation) otherObject;
      return getPort() == that.getPort() && Objects.equals(getAddress(), that.getAddress());
  }

  @Override
  public int hashCode() {
      return Objects.hash(getAddress(), getPort());
  }
}
