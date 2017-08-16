/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.record;

import java.nio.ByteBuffer;

import org.apache.kafka.common.header.Header;

/**
 * A log record is a tuple consisting of a unique offset in the log, a sequence number assigned by
 * the producer, a timestamp, a key and a value.
 */
public interface Record {

    Header[] EMPTY_HEADERS = new Header[0];

    /**
     * The offset of this record in the log
     * @return the offset
     */
    long offset();

    /**
     * Get the sequence number assigned by the producer.
     * @return the sequence number
     */
    int sequence();

    /**
     * Get the size in bytes of this record.
     * @return the size of the record in bytes
     */
    int sizeInBytes();

    /**
     * Get the record's timestamp.
     * @return the record's timestamp
     */
    long timestamp();

    /**
     * Get a checksum of the record contents.
     * @return A 4-byte unsigned checksum represented as a long or null if the message format does not
     *         include a checksum (i.e. for v2 and above)
     */
    Long checksumOrNull();

    /**
     * Check whether the record has a valid checksum.
     * @return true if the record has a valid checksum, false otherwise
     */
<<<<<<< HEAD
    boolean isValid();
=======
    public Record(long timestamp, byte[] key, byte[] value, CompressionType type, int valueOffset, int valueSize) {
        this(ByteBuffer.allocate(recordSize(key == null ? 0 : key.length,
            value == null ? 0 : valueSize >= 0 ? valueSize : value.length - valueOffset)));
        write(this.buffer, timestamp, key, value, type, valueOffset, valueSize);
        this.buffer.rewind();
    }

    public Record(long timestamp, byte[] key, byte[] value, CompressionType type) {
        this(timestamp, key, value, type, 0, -1);
    }

    public Record(long timestamp, byte[] value, CompressionType type) {
        this(timestamp, null, value, type);
    }

    public Record(long timestamp, byte[] key, byte[] value) {
        this(timestamp, key, value, CompressionType.NONE);
    }

    public Record(long timestamp, byte[] value) {
        this(timestamp, null, value, CompressionType.NONE);
    }

    // Write a record to the buffer, if the record's compression type is none, then
    // its value payload should be already compressed with the specified type
    public static void write(ByteBuffer buffer, long timestamp, byte[] key, byte[] value, CompressionType type, int valueOffset, int valueSize) {
        // construct the compressor with compression type none since this function will not do any
        //compression according to the input type, it will just write the record's payload as is
        Compressor compressor = new Compressor(buffer, CompressionType.NONE);
        try {
            compressor.putRecord(timestamp, key, value, type, valueOffset, valueSize);
        } finally {
            compressor.close();
        }
    }

    public static void write(Compressor compressor, long crc, byte attributes, long timestamp, byte[] key, byte[] value, int valueOffset, int valueSize) {
        // write crc
        compressor.putInt((int) (crc & 0xffffffffL));
        // write magic value
        compressor.putByte(CURRENT_MAGIC_VALUE);
        // write attributes
        compressor.putByte(attributes);
        // write timestamp
        compressor.putLong(timestamp);
        // write the key
        if (key == null) {
            compressor.putInt(-1);
        } else {
            compressor.putInt(key.length);
            compressor.put(key, 0, key.length);
        }
        // write the value
        if (value == null) {
            compressor.putInt(-1);
        } else {
            int size = valueSize >= 0 ? valueSize : (value.length - valueOffset);
            compressor.putInt(size);
            compressor.put(value, valueOffset, size);
        }
    }

    public static int recordSize(byte[] key, byte[] value) {
        return recordSize(key == null ? 0 : key.length, value == null ? 0 : value.length);
    }

    public static int recordSize(int keySize, int valueSize) {
        return CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTE_LENGTH + TIMESTAMP_LENGTH + KEY_SIZE_LENGTH + keySize + VALUE_SIZE_LENGTH + valueSize;
    }

    public ByteBuffer buffer() {
        return this.buffer;
    }

    public static byte computeAttributes(CompressionType type) {
        byte attributes = 0;
        if (type.id > 0)
            attributes = (byte) (attributes | (COMPRESSION_CODEC_MASK & type.id));
        return attributes;
    }
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3

    /**
     * Raise a {@link org.apache.kafka.common.errors.CorruptRecordException} if the record does not have a valid checksum.
     */
    void ensureValid();

    /**
     * Get the size in bytes of the key.
     * @return the size of the key, or -1 if there is no key
     */
    int keySize();

    /**
     * Check whether this record has a key
     * @return true if there is a key, false otherwise
     */
    boolean hasKey();

    /**
     * Get the record's key.
     * @return the key or null if there is none
     */
    ByteBuffer key();

    /**
     * Get the size in bytes of the value.
     * @return the size of the value, or -1 if the value is null
     */
    int valueSize();

    /**
     * Check whether a value is present (i.e. if the value is not null)
     * @return true if so, false otherwise
     */
    boolean hasValue();

    /**
     * Get the record's value
     * @return the (nullable) value
     */
    ByteBuffer value();

    /**
     * Check whether the record has a particular magic. For versions prior to 2, the record contains its own magic,
     * so this function can be used to check whether it matches a particular value. For version 2 and above, this
     * method returns true if the passed magic is greater than or equal to 2.
     *
     * @param magic the magic value to check
     * @return true if the record has a magic field (versions prior to 2) and the value matches
     */
    boolean hasMagic(byte magic);

    /**
     * For versions prior to 2, check whether the record is compressed (and therefore
     * has nested record content). For versions 2 and above, this always returns false.
     * @return true if the magic is lower than 2 and the record is compressed
     */
    boolean isCompressed();

    /**
     * For versions prior to 2, the record contained a timestamp type attribute. This method can be
     * used to check whether the value of that attribute matches a particular timestamp type. For versions
     * 2 and above, this will always be false.
     *
     * @param timestampType the timestamp type to compare
     * @return true if the version is lower than 2 and the timestamp type matches
     */
    boolean hasTimestampType(TimestampType timestampType);

    /**
     * Get the headers. For magic versions 1 and below, this always returns an empty array.
     *
     * @return the array of headers
     */
    Header[] headers();
}
