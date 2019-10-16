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
import java.util.List;

public class GetChildrenResponse implements Record, Comparable<GetChildrenResponse> {
    private List<String> children;
    public GetChildrenResponse() {
    }
    public GetChildrenResponse(List<String> children) {
        this.children=children;
    }
    public List<String> getChildren() {
        return children;
    }
    public void setChildren(List<String> m_) {
        children = m_;
    }
    public void serialize(OutputArchive a_, String tag) throws IOException {
        a_.startRecord(this,tag);
        {
            a_.startVector(children,"children");
            if (children!= null) {
                for (String e1 : children) {
                    a_.writeString(e1, "e1");
                }
            }
            a_.endVector(children,"children");
        }
        a_.endRecord(this,tag);
    }
    public void deserialize(InputArchive a_, String tag) throws IOException {
        a_.startRecord(tag);
        {
            Index vidx1 = a_.startVector("children");
            if (vidx1!= null) {
                children=new java.util.ArrayList<>();
                for (; !vidx1.done(); vidx1.incr()) {
                    String e1;
                    e1=a_.readString("e1");
                    children.add(e1);
                }
            }
            a_.endVector("children");
        }
        a_.endRecord(tag);
    }
    @Override
    public String toString() {
        try {
            final ByteArrayOutputStream s = new ByteArrayOutputStream();
            final CsvOutputArchive a_ = new CsvOutputArchive(s);
            a_.startRecord(this,"");
            {
                a_.startVector(children,"children");
                if (children!= null) {
                    for (String e1 : children) {
                        a_.writeString(e1, "e1");
                    }
                }
                a_.endVector(children,"children");
            }
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
    public int compareTo(GetChildrenResponse ignored) throws ClassCastException {
        throw new UnsupportedOperationException("comparing GetChildrenResponse is unimplemented");
    }
    @Override
    public boolean equals(Object peer_) {
        if (!(peer_ instanceof GetChildrenResponse)) {
            return false;
        } else if (peer_ == this) {
            return true;
        } else {
            return children.equals(((GetChildrenResponse) peer_).getChildren());
        }
    }
    @Override
    public int hashCode() {
        int ret = children.hashCode();
        return 37*17 + ret;
    }

    public static String signature() {
        return "LGetChildrenResponse([s])";
    }
}
