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

import java.nio.charset.StandardCharsets;
import java.util.Comparator;

import org.apache.jute_voltpatches.*;

public class ExistsRequest implements Record {
    private String path;
    private boolean watch;
    public ExistsRequest() {
    }
    public ExistsRequest(String path, boolean watch) {
        this.path=path;
        this.watch=watch;
    }
    public String getPath() {
        return path;
    }
    public void setPath(String m_) {
        path=m_;
    }
    public boolean getWatch() {
        return watch;
    }
    public void setWatch(boolean m_) {
        watch=m_;
    }
    public void serialize(OutputArchive a_, String tag) throws java.io.IOException {
        a_.startRecord(this,tag);
        a_.writeString(path,"path");
        a_.writeBool(watch,"watch");
        a_.endRecord(this,tag);
    }
    public void deserialize(InputArchive a_, String tag) throws java.io.IOException {
        a_.startRecord(tag);
        path=a_.readString("path");
        watch=a_.readBool("watch");
        a_.endRecord(tag);
    }
    @Override
    public String toString() {
        try {
            final java.io.ByteArrayOutputStream s = new java.io.ByteArrayOutputStream();
            final CsvOutputArchive a_ = new CsvOutputArchive(s);
            a_.startRecord(this,"");
            a_.writeString(path,"path");
            a_.writeBool(watch,"watch");
            a_.endRecord(this,"");
            return new String(s.toByteArray(), StandardCharsets.UTF_8);
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
        return "ERROR";
    }
    public void write(java.io.DataOutput out) throws java.io.IOException {
        serialize(new BinaryOutputArchive(out), "");
    }
    public void readFields(java.io.DataInput in) throws java.io.IOException {
        deserialize(new BinaryInputArchive(in), "");
    }
    public int compareTo (Object peer_) throws ClassCastException {
        if (! (peer_ instanceof ExistsRequest)) {
            throw new ClassCastException("Comparing different types of records.");
        } else {
            return Comparator.comparing(ExistsRequest::getPath)
                    .thenComparing(ExistsRequest::getWatch)
                    .compare(this, (ExistsRequest) peer_);
        }
    }
    @Override
    public boolean equals(Object peer_) {
        return peer_ instanceof ExistsRequest && compareTo(peer_) == 0;
    }
    @Override
    public int hashCode() {
        int result = 17;
        int ret;
        ret = path.hashCode();
        result = 37*result + ret;
        ret = (watch)?0:1;
        result = 37*result + ret;
        return result;
    }
    public static String signature() {
        return "LExistsRequest(sz)";
    }
}
