package de.tum.i13.server.kv.commandprocessing;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.state.ECSServerState;
import de.tum.i13.shared.CommandProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HashRingCommandProcessor implements CommandProcessor<KVMessage> {

    private static final Logger LOGGER = LogManager.getLogger(HashRingCommandProcessor.class);
    private final ECSServerState serverState;

    public HashRingCommandProcessor(ECSServerState serverState) {
        this.serverState = serverState;
    }


    @Override
    public KVMessage process(KVMessage command) {
        return command.getStatus().equals(KVMessage.StatusType.KEYRANGE) ? this.keyRange() : null;
    }

    private KVMessage keyRange() {
        final String writeRanges = this.serverState.getRingMetadata().packWriteRanges();
        LOGGER.info("Sending key-range to client: {}", writeRanges);

        return new KVMessageImpl(writeRanges, KVMessage.StatusType.KEYRANGE_SUCCESS);
    }

}
