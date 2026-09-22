package org.example.Stream;

import org.example.Protocol.RespParser;
import org.example.Stream.StreamStore;
import org.example.commond.RespCommandEncoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StreamSnapshotFile {

    private Path streamPath(Path rdbPath) {
        return rdbPath.resolveSibling(
                rdbPath.getFileName() + ".streams"
        );
    }

    // RAM → disk
    public void save(
            Path rdbPath,
            StreamStore store
    ) throws IOException {

        Path path = streamPath(rdbPath);

        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }

        Path temp = path.resolveSibling(
                path.getFileName() + ".tmp"
        );

        try (OutputStream out = Files.newOutputStream(temp)) {

            for (List<String> command : store.snapshotCommands()) {

                out.write(
                        RespCommandEncoder.encode(command)
                );
            }
        }

        try {

            Files.move(
                    temp,
                    path,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
            );

        } catch (java.nio.file.AtomicMoveNotSupportedException e) {

            Files.move(
                    temp,
                    path,
                    StandardCopyOption.REPLACE_EXISTING
            );
        }

        System.out.println("Streams saved: " + path);
    }

    // Disk → RAM
    public void load(
            Path rdbPath,
            StreamStore store
    ) throws IOException {

        Path path = streamPath(rdbPath);

        if (!Files.exists(path)) {
            return;
        }

        RespParser parser = new RespParser();

        store.clear();

        try (InputStream input = Files.newInputStream(path)) {

            while (true) {

                List<String> command = parser.parse(input);

                if (command == null) {
                    break;
                }

                if (command.size() < 5 ||
                        !command.get(0).equalsIgnoreCase("XADD") ||
                        command.size() % 2 == 0) {

                    throw new IOException("Invalid stream snapshot");
                }

                Map<String, String> fields = new LinkedHashMap<>();

                for (int i = 3; i < command.size(); i += 2) {
                    fields.put(command.get(i), command.get(i + 1));
                }

                try {

                    store.add(
                            command.get(1),
                            command.get(2),
                            fields
                    );

                } catch (IllegalArgumentException e) {

                    throw new IOException(
                            "Invalid stream entry in snapshot",
                            e
                    );
                }
            }
        }

        System.out.println("Streams loaded: " + path);
    }
}