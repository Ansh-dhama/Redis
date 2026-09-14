package org.example.commond;

import java.util.List;

public interface Command {

    byte[] execute(List<String> args);
}