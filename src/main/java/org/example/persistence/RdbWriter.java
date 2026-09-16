package org.example.persistence;

import org.example.storage.RedisEntry;
import org.example.storage.RedisStore;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import java.util.Map;

public class RdbWriter {

    private static final int SELECT_DB = 0xFE;
    private static final int RESIZEDB = 0xFB;

    private static final int EXPIRE_MS = 0xFC;

    private static final int STRING_TYPE = 0x00;

    private static final int EOF = 0xFF;


    public void save(
            Path path,
            RedisStore store
    ) throws IOException {

        /*
         * Take an in-memory snapshot first.
         */
        Map<String, RedisEntry> snapshot =
                store.snapshot();


        /*
         * Make sure directory exists.
         */
        Path parent =
                path.getParent();

        if (parent != null) {
            Files.createDirectories(parent);
        }


        /*
         * Don't directly overwrite dump.rdb.
         *
         * First write:
         *
         * dump.rdb.tmp
         *
         * Then replace dump.rdb.
         */
        Path tempPath =
                Path.of(
                        path.toString() + ".tmp"
                );


        try (
                OutputStream output =
                        new BufferedOutputStream(
                                Files.newOutputStream(
                                        tempPath
                                )
                        )
        ) {

            /*
             * RDB HEADER
             *
             * REDIS0014
             */
            writeHeader(output);


            /*
             * SELECT DB 0
             */
            output.write(
                    SELECT_DB
            );

            writeLength(
                    output,
                    0
            );


            /*
             * Count keys having expiry.
             */
            long expiringCount =
                    snapshot.values()
                            .stream()
                            .filter(
                                    entry ->
                                            entry.getExpireAt()
                                                    != null
                            )
                            .count();


            /*
             * RESIZEDB
             *
             * total keys
             * expiring keys
             */
            output.write(
                    RESIZEDB
            );

            writeLength(
                    output,
                    snapshot.size()
            );

            writeLength(
                    output,
                    expiringCount
            );


            /*
             * Write every key/value.
             */
            for (
                    Map.Entry<String, RedisEntry> item :
                    snapshot.entrySet()
            ) {

                String key =
                        item.getKey();

                RedisEntry entry =
                        item.getValue();


                /*
                 * If key has expiry,
                 * write FC followed by
                 * absolute timestamp.
                 */
                if (entry.getExpireAt()
                        != null) {

                    output.write(
                            EXPIRE_MS
                    );

                    writeLittleEndian(
                            output,
                            entry.getExpireAt(),
                            8
                    );
                }


                /*
                 * Value type:
                 *
                 * 00 = Redis String
                 */
                output.write(
                        STRING_TYPE
                );


                writeString(
                        output,
                        key
                );


                writeString(
                        output,
                        entry.getValue()
                );
            }


            /*
             * End of RDB.
             */
            output.write(
                    EOF
            );

            output.flush();
        }


        /*
         * Only replace the real file after
         * successful writing.
         */
        Files.move(
                tempPath,
                path,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
        );


        System.out.println(
                "RDB saved successfully: "
                        + path
        );
    }


    /*
     * --------------------------------------
     * HEADER
     * --------------------------------------
     */

    private void writeHeader(
            OutputStream output
    ) throws IOException {

        output.write(
                "REDIS0014"
                        .getBytes(
                                StandardCharsets.US_ASCII
                        )
        );
    }


    /*
     * --------------------------------------
     * STRING
     * --------------------------------------
     */

    private void writeString(
            OutputStream output,
            String value
    ) throws IOException {

        byte[] bytes =
                value.getBytes(
                        StandardCharsets.UTF_8
                );


        /*
         * First write string length.
         */
        writeLength(
                output,
                bytes.length
        );


        /*
         * Then write actual bytes.
         */
        output.write(
                bytes
        );
    }


    /*
     * --------------------------------------
     * RDB LENGTH ENCODING
     * --------------------------------------
     */

    private void writeLength(
            OutputStream output,
            long length
    ) throws IOException {

        /*
         * 6 bit:
         *
         * 00xxxxxx
         *
         * 0 → 63
         */
        if (length < 64) {

            output.write(
                    (int) length
            );

            return;
        }


        /*
         * 14 bit:
         *
         * 01xxxxxx xxxxxxxx
         *
         * up to 16383.
         */
        if (length < 16384) {

            int first =
                    (int)
                            (
                                    0x40
                                    |
                                    ((length >> 8)
                                            & 0x3F)
                            );


            int second =
                    (int)
                            (length & 0xFF);


            output.write(first);
            output.write(second);

            return;
        }


        /*
         * 32-bit length.
         *
         * 0x80 tells the parser:
         * next 4 bytes contain length.
         */
        output.write(
                0x80
        );


        writeBigEndian(
                output,
                length,
                4
        );
    }


    /*
     * --------------------------------------
     * LITTLE ENDIAN
     *
     * Used by expiry timestamps.
     * --------------------------------------
     */

    private void writeLittleEndian(
            OutputStream output,
            long value,
            int byteCount
    ) throws IOException {

        for (
                int i = 0;
                i < byteCount;
                i++
        ) {

            output.write(
                    (int)
                            (
                                    (value >> (8 * i))
                                    & 0xFF
                            )
            );
        }
    }


    /*
     * --------------------------------------
     * BIG ENDIAN
     *
     * Used for larger length values.
     * --------------------------------------
     */

    private void writeBigEndian(
            OutputStream output,
            long value,
            int byteCount
    ) throws IOException {

        for (
                int i = byteCount - 1;
                i >= 0;
                i--
        ) {

            output.write(
                    (int)
                            (
                                    (value >> (8 * i))
                                    & 0xFF
                            )
            );
        }
    }
}