package org.example.Stream;

import java.util.ArrayList;
import java.util.List;

public class TransactionState {

    private boolean active = false;

    private final List<List<String>> queue =
            new ArrayList<>();


    public boolean isActive() {
        return active;
    }


    // MULTI
    public void begin() {
        active = true;
        queue.clear();
    }


    // Queue commands after MULTI
    public void add(List<String> command) {
        queue.add(List.copyOf(command));
    }


    // EXEC
    public List<List<String>> drain() {

        List<List<String>> commands =
                new ArrayList<>(queue);

        queue.clear();
        active = false;

        return commands;
    }


    // DISCARD
    public void discard() {
        queue.clear();
        active = false;
    }
}