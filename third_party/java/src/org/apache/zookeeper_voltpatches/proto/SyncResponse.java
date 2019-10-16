// File generated by hadoop record compiler. Do not edit.
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

package org.apache.zookeeper_voltpatches.proto;

import org.apache.jute_voltpatches.*;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;

public class SyncResponse implements Record, Comparable<SyncResponse> {
    private String path;
    public SyncResponse() {
    }
    public SyncResponse(
            String path) {
        this.path = path;
    }
    public String getPath() {
        return path;
    }
    public void setPath(String m_) {
        path = m_;
    }
    public void serialize(OutputArchive a_, String tag) throws IOException {
        a_.startRecord(this,tag);
        a_.writeString(path,"path");
        a_.endRecord(this,tag);
    }
    public void deserialize(InputArchive a_, String tag) throws IOException {
        a_.startRecord(tag);
        path = a_.readString("path");
        a_.endRecord(tag);
    }
    @Override
    public String toString() {
        try {
            final ByteArrayOutputStream s = new ByteArrayOutputStream();
            final CsvOutputArchive a_ = new CsvOutputArchive(s);
            a_.startRecord(this,"");
            a_.writeString(path,"path");
            a_.endRecord(this,"");
            return new String(s.toByteArray(), StandardCharsets.UTF_8);
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
        return "ERROR";
    }
    public void write(DataOutput out) throws IOException {
        serialize(new BinaryOutputArchive(out), "");
    }
    public void readFields(DataInput in) throws IOException {
        deserialize(new BinaryInputArchive(in), "");
    }
    @Override
    public int compareTo(SyncResponse peer_) {
        return Comparator.comparing(SyncResponse::getPath).compare(this, peer_);
    }
    @Override
    public boolean equals(Object peer_) {
        if (! (peer_ instanceof SyncResponse)) {
            return false;
        } else if (peer_ == this) {
            return true;
        } else {
            return compareTo((SyncResponse) peer_) == 0;
        }
    }
    @Override
    public int hashCode() {
        int result = 17;
        return 37*result + path.hashCode();
    }
    public static String signature() {
        return "LSyncResponse(s)";
    }
}
