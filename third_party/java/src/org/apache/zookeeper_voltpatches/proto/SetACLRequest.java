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
import org.apache.zookeeper_voltpatches.data.ACL;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SetACLRequest implements Record, Comparable<SetACLRequest> {
    private String path;
    private List<ACL> acl;
    private int version;
    public SetACLRequest() {
    }
    public SetACLRequest(String path, List<ACL> acl, int version) {
        this.path = path;
        this.acl = acl;
        this.version = version;
    }
    public String getPath() {
        return path;
    }
    public void setPath(String m_) {
        path = m_;
    }
    public List<ACL> getAcl() {
        return acl;
    }
    public void setAcl(List<ACL> m_) {
        acl = m_;
    }
    public int getVersion() {
        return version;
    }
    public void setVersion(int m_) {
        version = m_;
    }
    public void serialize(OutputArchive a_, String tag) throws IOException {
        a_.startRecord(this,tag);
        a_.writeString(path,"path");
        {
            a_.startVector(acl,"acl");
            if (acl != null) {
                for (ACL e1 : acl) {
                    a_.writeRecord(e1, "e1");
                }
            }
            a_.endVector(acl,"acl");
        }
        a_.writeInt(version,"version");
        a_.endRecord(this,tag);
    }
    public void deserialize(InputArchive a_, String tag) throws IOException {
        a_.startRecord(tag);
        path = a_.readString("path");
        {
            Index vidx1 = a_.startVector("acl");
            if (vidx1!= null) {
                acl = new ArrayList<>();
                for (; !vidx1.done(); vidx1.incr()) {
                    final ACL e1 = new ACL();
                    a_.readRecord(e1,"e1");
                    acl.add(e1);
                }
            }
            a_.endVector("acl");
        }
        version=a_.readInt("version");
        a_.endRecord(tag);
    }
    @Override
    public String toString() {
        try {
            final ByteArrayOutputStream s = new ByteArrayOutputStream();
            final CsvOutputArchive a_ = new CsvOutputArchive(s);
            a_.startRecord(this,"");
            a_.writeString(path,"path");
            {
                a_.startVector(acl,"acl");
                if (acl!= null) {
                    for (ACL e1 : acl) {
                        a_.writeRecord(e1, "e1");
                    }
                }
                a_.endVector(acl,"acl");
            }
            a_.writeInt(version,"version");
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
    public int compareTo (SetACLRequest peer_) throws ClassCastException {
        throw new UnsupportedOperationException("comparing SetACLRequest is unimplemented");
    }
    @Override
    public boolean equals(Object peer_) {
        if (!(peer_ instanceof SetACLRequest)) {
            return false;
        } else if (peer_ == this) {
            return true;
        } else {
            SetACLRequest peer = (SetACLRequest) peer_;
            return path.equals(peer.getPath()) &&
                    acl.equals(peer.getAcl()) &&
                    version == peer.getVersion();
        }
    }
    @Override
    public int hashCode() {
        int result = 17;
        int ret;
        ret = path.hashCode();
        result = 37*result + ret;
        ret = acl.hashCode();
        result = 37*result + ret;
        ret = version;
        result = 37*result + ret;
        return result;
    }
    public static String signature() {
        return "LSetACLRequest(s[LACL(iLId(ss))]i)";
    }
}
