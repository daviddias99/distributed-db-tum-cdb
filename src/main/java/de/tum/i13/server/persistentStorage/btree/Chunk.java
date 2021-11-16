package de.tum.i13.server.persistentStorage.btree;

interface Chunk<V> {

  public int findIndexOfFirstGreaterThen(String k);

  public Pair<V> get(int index);

  public Pair<V> remove(int index);

  public Pair<V> set(int index,  Pair<V> element);

  public void shiftRightOne(int startIndex, int keyCount);
  
  public void shiftLeftOne(int startIndex, int keyCount);

  public int shiftRightOneAfterFirstGreaterThan(String key, int keyCount);
}
