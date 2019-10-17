/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jute_voltpatches;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Interface that is implemented by generated classes.
 *
 */
public interface Record<T> extends Comparable<T> {
    void serialize(OutputArchive archive, String tag) throws IOException;
    void deserialize(InputArchive archive, String tag) throws IOException;

    abstract class AbstractRecord<T> implements Record<T> {
        public void write(DataOutput out) throws IOException {
            serialize(new BinaryOutputArchive(out), "");
        }
        public void readFields(DataInput in) throws IOException {
            deserialize(new BinaryInputArchive(in), "");
        }
        @Override
        public String toString() {
            try {
                final ByteArrayOutputStream s = new ByteArrayOutputStream();
                serialize(new CsvOutputArchive(s), "");
                return new String(s.toByteArray(), StandardCharsets.UTF_8);
            } catch (Throwable ex) {
                ex.printStackTrace();
            }
            return "ERROR";
        }
        @SuppressWarnings("unchecked")
        public boolean equalsHelper(Object peer_) {
            // Assumes that peer_ is a T.
            return peer_ == this || compareTo((T) peer_) == 0;
        }
    }
}
