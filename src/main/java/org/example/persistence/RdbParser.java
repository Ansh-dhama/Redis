package org.example.persistence;

import org.example.storage.RedisStore;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class RdbParser {

    private static final int AUX = 0xFA;
    private static final int RESIZEDB = 0xFB;
    private static final int EXPIRE_MS = 0xFC;
    private static final int EXPIRE_SEC = 0xFD;
    private static final int SELECT_DB = 0xFE;
    private static final int EOF = 0xFF;

    private static final int STRING = 0x00;


    public void load(
            Path path,
            RedisStore store
    ) throws IOException {

        if (!Files.exists(path)) {

            System.out.println(
                    "RDB file not found: " + path
            );

            return;
        }


        try (
                InputStream input =
                        new BufferedInputStream(
                                Files.newInputStream(path)
                        )
        ) {

            readHeader(input);

            Long expiry = null;


            while (true) {

                int type = input.read();


                if (type == -1 || type == EOF) {
                    break;
                }


                switch (type) {

                    case AUX -> {

                        String key =
                                readString(input);

                        String value =
                                readString(input);

                        System.out.println(
                                "AUX: "
                                        + key
                                        + "="
                                        + value
                        );
                    }


                    case SELECT_DB -> {

                        long db =
                                readLength(input);

                        System.out.println(
                                "Loading DB: " + db
                        );
                    }


                    case RESIZEDB -> {

                        long total =
                                readLength(input);

                        long expiring =
                                readLength(input);

                        System.out.println(
                                "Keys: " + total
                        );
                    }


                    case EXPIRE_MS -> {

                        expiry =
                                readLittleEndian(
                                        input,
                                        8
                                );
                    }


                    case EXPIRE_SEC -> {

                        expiry =
                                readLittleEndian(
                                        input,
                                        4
                                ) * 1000L;
                    }


                    default -> {

                        if (type != STRING) {

                            throw new IOException(
                                    "Unsupported Redis type: "
                                            + type
                            );
                        }


                        String key =
                                readString(input);

                        String value =
                                readString(input);


                        store.restore(
                                key,
                                value,
                                expiry
                        );


                        System.out.println(
                                "Loaded key: " + key
                        );


                        expiry = null;
                    }
                }
            }
        }
    }


    private void readHeader(
            InputStream input
    ) throws IOException {

        byte[] header =
                input.readNBytes(9);


        String value =
                new String(
                        header,
                        StandardCharsets.US_ASCII
                );


        if (!value.startsWith("REDIS")) {

            throw new IOException(
                    "Invalid RDB file"
            );
        }


        System.out.println(
                "RDB version: "
                        + value.substring(5)
        );
    }


    private String readString(
            InputStream input
    ) throws IOException {

        int first =
                input.read();


        if (first == -1) {
            throw new IOException("Unexpected EOF");
        }


        int prefix =
                (first & 0xC0) >> 6;


        /*
         * 00xxxxxx
         * Small normal string
         */
        if (prefix == 0) {

            int length =
                    first & 0x3F;

            return readBytes(
                    input,
                    length
            );
        }


        /*
         * 01xxxxxx
         * 14-bit length
         */
        if (prefix == 1) {

            int second =
                    input.read();

            int length =
                    ((first & 0x3F) << 8)
                            | second;


            return readBytes(
                    input,
                    length
            );
        }


        /*
         * 11xxxxxx
         * Integer encoded string
         */
        if (prefix == 3) {

            int encoding =
                    first & 0x3F;


            if (encoding == 0) {

                return Integer.toString(
                        (byte) input.read()
                );
            }


            if (encoding == 1) {

                return Short.toString(
                        (short)
                                readLittleEndian(
                                        input,
                                        2
                                )
                );
            }


            if (encoding == 2) {

                return Integer.toString(
                        (int)
                                readLittleEndian(
                                        input,
                                        4
                                )
                );
            }


            throw new IOException(
                    "Compressed strings not supported yet"
            );
        }


        throw new IOException(
                "Unsupported string length encoding"
        );
    }


    private long readLength(
            InputStream input
    ) throws IOException {

        int first =
                input.read();


        int prefix =
                (first & 0xC0) >> 6;


        if (prefix == 0) {

            return first & 0x3F;
        }


        if (prefix == 1) {

            int second =
                    input.read();

            return ((long)
                    (first & 0x3F)
                    << 8)
                    | second;
        }


        if (prefix == 2) {

            return readBigEndian(
                    input,
                    4
            );
        }


        throw new IOException(
                "Invalid length"
        );
    }


    private String readBytes(
            InputStream input,
            int length
    ) throws IOException {

        byte[] bytes =
                input.readNBytes(length);


        if (bytes.length != length) {

            throw new IOException(
                    "Unexpected EOF"
            );
        }


        return new String(
                bytes,
                StandardCharsets.UTF_8
        );
    }


    private long readLittleEndian(
            InputStream input,
            int count
    ) throws IOException {

        long result = 0;


        for (int i = 0; i < count; i++) {

            int value =
                    input.read();


            result |=
                    ((long) value)
                            << (8 * i);
        }


        return result;
    }


    private long readBigEndian(
            InputStream input,
            int count
    ) throws IOException {

        long result = 0;


        for (int i = 0; i < count; i++) {

            result =
                    (result << 8)
                            | input.read();
        }


        return result;
    }
}