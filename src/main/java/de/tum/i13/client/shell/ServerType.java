package de.tum.i13.client.shell;

/**
 * The type of server a client can connect to.
 */
enum ServerType {
    /**
     * A type of server that is controlled by an {@link de.tum.i13.ecs.ExternalConfigurationServer}.
     * Supports the {@link de.tum.i13.server.kv.KVMessage.StatusType#KEYRANGE} command.
     */
    ECS,
    /**
     * A type of server that is not externally controlled, but works in a peer to peer fashion according to Chord
     * protocol
     *
     * @see <a href="https://doi.org/10.1145/964723.383071">Chord paper</a>
     */
    CHORD
}
