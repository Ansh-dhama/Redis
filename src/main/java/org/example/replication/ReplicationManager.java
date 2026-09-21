package org.example.replication;

import org.example.commond.RespCommandEncoder;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicationManager {

    private final String replicationId =
            UUID.randomUUID().toString().replace("-", "");

    private final AtomicLong masterOffset =
            new AtomicLong(0);

    private final Map<Socket, ReplicaState> replicas =
            new ConcurrentHashMap<>();

    private final Object ackMonitor = new Object();


    // Used by PSYNC and INFO
    public String getReplicationId() {
        return replicationId;
    }

    public long getMasterOffset() {
        return masterOffset.get();
    }

    public int getReplicaCount() {
        return replicas.size();
    }


    // Register a replica after PSYNC
    public void registerReplica(Socket socket, OutputStream output) {
        replicas.put(socket, new ReplicaState(output));
    }

    public void removeReplica(Socket socket) {
        replicas.remove(socket);
    }


    // Master sends SET command to replicas
    public void propagate(byte[] command) {

        masterOffset.addAndGet(command.length);

        for (var entry : replicas.entrySet()) {
            try {
                send(entry.getValue(), command);
            } catch (IOException e) {
                removeReplica(entry.getKey());
            }
        }
    }


    // Master asks replicas for their current offsets
    public void requestAcks() {

        byte[] command = RespCommandEncoder.encode(
                java.util.List.of("REPLCONF", "GETACK", "*")
        );

        masterOffset.addAndGet(command.length);

        for (var entry : replicas.entrySet()) {
            try {
                send(entry.getValue(), command);
            } catch (IOException e) {
                removeReplica(entry.getKey());
            }
        }
    }


    // Called when master receives REPLCONF ACK <offset>
    public void updateAck(Socket socket, long offset) {

        ReplicaState replica = replicas.get(socket);

        if (replica == null) return;

        replica.ackOffset = offset;

        synchronized (ackMonitor) {
            ackMonitor.notifyAll();
        }
    }


    // Count replicas that processed at least requiredOffset
    public int countAckedReplicas(long requiredOffset) {

        int count = 0;

        for (ReplicaState replica : replicas.values()) {
            if (replica.ackOffset >= requiredOffset) {
                count++;
            }
        }

        return count;
    }


    // Wait until enough replicas acknowledge, or timeout occurs
    public int waitForReplicas(
            int requiredReplicas,
            long requiredOffset,
            long timeoutMs
    ) {

        long deadline = System.currentTimeMillis() + timeoutMs;

        synchronized (ackMonitor) {

            while (true) {

                int count = countAckedReplicas(requiredOffset);

                if (count >= requiredReplicas) {
                    return count;
                }

                long remaining = deadline - System.currentTimeMillis();

                if (remaining <= 0) {
                    return count;
                }

                try {
                    ackMonitor.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return count;
                }
            }
        }
    }


    // Prevent multiple threads from mixing bytes
    private void send(ReplicaState replica, byte[] command)
            throws IOException {

        synchronized (replica.output) {
            replica.output.write(command);
            replica.output.flush();
        }
    }


    private static class ReplicaState {

        final OutputStream output;

        volatile long ackOffset = -1;

        ReplicaState(OutputStream output) {
            this.output = output;
        }
    }
}