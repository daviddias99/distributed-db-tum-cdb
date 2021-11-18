package de.tum.i13.server.kv;

public class KVMessageForStub implements KVMessage{

    private StatusType type;
    private String key;
    private String value;

    public KVMessageForStub(StatusType type){
        this.type = type;
    }

    public KVMessageForStub(StatusType type, String key, String value){
        this.type = type;
        this.key = key;
        this.value = value;
    }

    @Override
    public String getKey() {
        return this.key;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public StatusType getStatus() {
        return this.type;
    }
    
}
