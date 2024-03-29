/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.IOException;

/**
 */
@SuppressWarnings("deprecation")
public class Version {

    // The logic for ID is: XXYYZZAA, where XX is major version, YY is minor version, ZZ is revision, and AA is Beta/RC indicator
    // AA values below 50 are beta builds, and below 99 are RC builds, with 99 indicating a release
    // the (internal) format of the id is there so we can easily do after/before checks on the id

    public static final int V_0_90_0_Beta1_ID = /*00*/900001;
    public static final Version V_0_90_0_Beta1 = new Version(V_0_90_0_Beta1_ID, false, org.apache.lucene.util.Version.LUCENE_4_1);
    public static final int V_0_90_0_RC1_ID = /*00*/900051;
    public static final Version V_0_90_0_RC1 = new Version(V_0_90_0_RC1_ID, false, org.apache.lucene.util.Version.LUCENE_4_1);
    public static final int V_0_90_0_RC2_ID = /*00*/900052;
    public static final Version V_0_90_0_RC2 = new Version(V_0_90_0_RC2_ID, false, org.apache.lucene.util.Version.LUCENE_4_2);
    public static final int V_0_90_0_ID = /*00*/900099;
    public static final Version V_0_90_0 = new Version(V_0_90_0_ID, false, org.apache.lucene.util.Version.LUCENE_4_2);
    public static final int V_0_90_1_ID = /*00*/900199;
    public static final Version V_0_90_1 = new Version(V_0_90_1_ID, false, org.apache.lucene.util.Version.LUCENE_4_3);
    public static final int V_0_90_2_ID = /*00*/900299;
    public static final Version V_0_90_2 = new Version(V_0_90_2_ID, false, org.apache.lucene.util.Version.LUCENE_4_3);
    public static final int V_0_90_3_ID = /*00*/900399;
    public static final Version V_0_90_3 = new Version(V_0_90_3_ID, false, org.apache.lucene.util.Version.LUCENE_4_4);
    public static final int V_0_90_4_ID = /*00*/900499;
    public static final Version V_0_90_4 = new Version(V_0_90_4_ID, false, org.apache.lucene.util.Version.LUCENE_4_4);
    public static final int V_0_90_5_ID = /*00*/900599;
    public static final Version V_0_90_5 = new Version(V_0_90_5_ID, false, org.apache.lucene.util.Version.LUCENE_4_4);
    public static final int V_0_90_6_ID = /*00*/900699;
    public static final Version V_0_90_6 = new Version(V_0_90_6_ID, false, org.apache.lucene.util.Version.LUCENE_4_5);
    public static final int V_0_90_7_ID = /*00*/900799;
    public static final Version V_0_90_7 = new Version(V_0_90_7_ID, false, org.apache.lucene.util.Version.LUCENE_4_5);
    public static final int V_0_90_8_ID = /*00*/900899;
    public static final Version V_0_90_8 = new Version(V_0_90_8_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_0_90_9_ID = /*00*/900999;
    public static final Version V_0_90_9 = new Version(V_0_90_9_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_0_90_10_ID = /*00*/901099;
    public static final Version V_0_90_10 = new Version(V_0_90_10_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_0_90_11_ID = /*00*/901199;
    public static final Version V_0_90_11 = new Version(V_0_90_11_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_0_90_12_ID = /*00*/901299;
    public static final Version V_0_90_12 = new Version(V_0_90_12_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_0_90_13_ID = /*00*/901399;
    public static final Version V_0_90_13 = new Version(V_0_90_13_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_0_90_14_ID = /*00*/901499;
    public static final Version V_0_90_14 = new Version(V_0_90_14_ID, true, org.apache.lucene.util.Version.LUCENE_4_6);

    public static final int V_1_0_0_Beta1_ID = 1000001;
    public static final Version V_1_0_0_Beta1 = new Version(V_1_0_0_Beta1_ID, false, org.apache.lucene.util.Version.LUCENE_4_5);
    public static final int V_1_0_0_Beta2_ID = 1000002;
    public static final Version V_1_0_0_Beta2 = new Version(V_1_0_0_Beta2_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_1_0_0_RC1_ID = 1000051;
    public static final Version V_1_0_0_RC1 = new Version(V_1_0_0_RC1_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_1_0_0_RC2_ID = 1000052;
    public static final Version V_1_0_0_RC2 = new Version(V_1_0_0_RC2_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_1_0_0_ID = 1000099;
    public static final Version V_1_0_0 = new Version(V_1_0_0_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_1_0_1_ID = 1000199;
    public static final Version V_1_0_1 = new Version(V_1_0_1_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_1_0_2_ID = 1000299;
    public static final Version V_1_0_2 = new Version(V_1_0_2_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_1_0_3_ID = 1000399;
    public static final Version V_1_0_3 = new Version(V_1_0_3_ID, false, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_1_0_4_ID = 1000499;
    public static final Version V_1_0_4 = new Version(V_1_0_4_ID, true, org.apache.lucene.util.Version.LUCENE_4_6);
    public static final int V_1_1_0_ID = 1010099;
    public static final Version V_1_1_0 = new Version(V_1_1_0_ID, false, org.apache.lucene.util.Version.LUCENE_4_7);
    public static final int V_1_1_1_ID = 1010199;
    public static final Version V_1_1_1 = new Version(V_1_1_1_ID, false, org.apache.lucene.util.Version.LUCENE_4_7);
    public static final int V_1_1_2_ID = 1010299;
    public static final Version V_1_1_2 = new Version(V_1_1_2_ID, false, org.apache.lucene.util.Version.LUCENE_4_7);
    public static final int V_1_2_0_ID = 1020099;
    public static final Version V_1_2_0 = new Version(V_1_2_0_ID, false, org.apache.lucene.util.Version.LUCENE_4_8);
    public static final int V_1_2_1_ID = 1020199;
    public static final Version V_1_2_1 = new Version(V_1_2_1_ID, false, org.apache.lucene.util.Version.LUCENE_4_8);
    public static final int V_1_2_2_ID = 1020299;
    public static final Version V_1_2_2 = new Version(V_1_2_2_ID, false, org.apache.lucene.util.Version.LUCENE_4_8);
    public static final int V_1_2_3_ID = 1020399;
    public static final Version V_1_2_3 = new Version(V_1_2_3_ID, false, org.apache.lucene.util.Version.LUCENE_4_8);
    public static final int V_1_2_4_ID = 1020499;
    public static final Version V_1_2_4 = new Version(V_1_2_4_ID, false, org.apache.lucene.util.Version.LUCENE_4_8);
    public static final int V_1_2_5_ID = 1020599;
    public static final Version V_1_2_5 = new Version(V_1_2_5_ID, true, org.apache.lucene.util.Version.LUCENE_4_8);
    public static final int V_1_3_0_ID = 1030099;
    public static final Version V_1_3_0 = new Version(V_1_3_0_ID, false, org.apache.lucene.util.Version.LUCENE_4_9);
    public static final int V_1_3_1_ID = 1030199;
    public static final Version V_1_3_1 = new Version(V_1_3_1_ID, false, org.apache.lucene.util.Version.LUCENE_4_9);
    public static final int V_1_3_2_ID = 1030299;
    public static final Version V_1_3_2 = new Version(V_1_3_2_ID, false, org.apache.lucene.util.Version.LUCENE_4_9);
    public static final int V_1_3_3_ID = 1030399;
    public static final Version V_1_3_3 = new Version(V_1_3_3_ID, false, org.apache.lucene.util.Version.LUCENE_4_9);
    public static final int V_1_3_4_ID = 1030499;
    public static final Version V_1_3_4 = new Version(V_1_3_4_ID, false, org.apache.lucene.util.Version.LUCENE_4_9);
    public static final int V_1_3_5_ID = 1030599;
    public static final Version V_1_3_5 = new Version(V_1_3_5_ID, false, org.apache.lucene.util.Version.LUCENE_4_9);
    public static final int V_1_3_6_ID = 1030699;
    public static final Version V_1_3_6 = new Version(V_1_3_6_ID, false, org.apache.lucene.util.Version.LUCENE_4_9);
    public static final int V_1_3_7_ID = 1030799;
    public static final Version V_1_3_7 = new Version(V_1_3_7_ID, false, org.apache.lucene.util.Version.LUCENE_4_9);
    public static final int V_1_3_8_ID = 1030899;
    public static final Version V_1_3_8 = new Version(V_1_3_8_ID, false, org.apache.lucene.util.Version.LUCENE_4_9);
    public static final int V_1_3_9_ID = 1030999;
    public static final Version V_1_3_9 = new Version(V_1_3_9_ID, false, org.apache.lucene.util.Version.LUCENE_4_9);
    public static final int V_1_3_10_ID = /*00*/1031099;
    public static final Version V_1_3_10 = new Version(V_1_3_10_ID, true, org.apache.lucene.util.Version.LUCENE_4_9);
    public static final int V_1_4_0_Beta1_ID = 1040001;
    public static final Version V_1_4_0_Beta1 = new Version(V_1_4_0_Beta1_ID, false, org.apache.lucene.util.Version.LUCENE_4_10_1);
    public static final int V_1_4_0_ID = 1040099;
    public static final Version V_1_4_0 = new Version(V_1_4_0_ID, false, org.apache.lucene.util.Version.LUCENE_4_10_2);
    public static final int V_1_4_1_ID = 1040199;
    public static final Version V_1_4_1 = new Version(V_1_4_1_ID, false, org.apache.lucene.util.Version.LUCENE_4_10_2);
    public static final int V_1_4_2_ID = 1040299;
    public static final Version V_1_4_2 = new Version(V_1_4_2_ID, false, org.apache.lucene.util.Version.LUCENE_4_10_2);
    public static final int V_1_4_3_ID = 1040399;
    public static final Version V_1_4_3 = new Version(V_1_4_3_ID, false, org.apache.lucene.util.Version.LUCENE_4_10_3);
    public static final int V_1_4_4_ID = 1040499;
    public static final Version V_1_4_4 = new Version(V_1_4_4_ID, false, org.apache.lucene.util.Version.LUCENE_4_10_3);
    public static final int V_1_4_5_ID = 1040599;
    public static final Version V_1_4_5 = new Version(V_1_4_5_ID, false, org.apache.lucene.util.Version.LUCENE_4_10_4);
    public static final int V_1_4_6_ID = 1040699;
    public static final Version V_1_4_6 = new Version(V_1_4_6_ID, true, org.apache.lucene.util.Version.LUCENE_4_10_4);
    public static final int V_1_5_0_ID = 1050099;
    public static final Version V_1_5_0 = new Version(V_1_5_0_ID, false, org.apache.lucene.util.Version.LUCENE_4_10_4);
    public static final int V_1_5_1_ID = 1050199;
    public static final Version V_1_5_1 = new Version(V_1_5_1_ID, false, org.apache.lucene.util.Version.LUCENE_4_10_4);
    public static final int V_1_5_2_ID = 1050299;
    public static final Version V_1_5_2 = new Version(V_1_5_2_ID, false, org.apache.lucene.util.Version.LUCENE_4_10_4);
    public static final int V_1_5_3_ID = 1050399;
    public static final Version V_1_5_3 = new Version(V_1_5_3_ID, true, org.apache.lucene.util.Version.LUCENE_4_10_4);
    public static final int V_1_6_0_ID = 1060099;
    public static final Version V_1_6_0 = new Version(V_1_6_0_ID, false, org.apache.lucene.util.Version.LUCENE_4_10_4);

    public static final Version CURRENT = V_1_6_0;

    static {
        assert CURRENT.luceneVersion.equals(Lucene.VERSION) : "Version must be upgraded to [" + Lucene.VERSION + "] is still set to [" + CURRENT.luceneVersion + "]";
    }

    public static Version readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static Version fromId(int id) {
        switch (id) {
            case V_1_6_0_ID:
                return V_1_6_0;
            case V_1_5_3_ID:
                return V_1_5_3;
            case V_1_5_2_ID:
                return V_1_5_2;
            case V_1_5_1_ID:
                return V_1_5_1;
            case V_1_5_0_ID:
                return V_1_5_0;
            case V_1_4_6_ID:
                return V_1_4_6;
            case V_1_4_5_ID:
                return V_1_4_5;
            case V_1_4_4_ID:
                return V_1_4_4;
            case V_1_4_3_ID:
                return V_1_4_3;
            case V_1_4_2_ID:
                return V_1_4_2;
            case V_1_4_1_ID:
                return V_1_4_1;
            case V_1_4_0_ID:
                 return V_1_4_0;
            case V_1_4_0_Beta1_ID:
                return V_1_4_0_Beta1;
            case V_1_3_10_ID:
                return V_1_3_10;
            case V_1_3_9_ID:
                return V_1_3_9;
            case V_1_3_8_ID:
                return V_1_3_8;
            case V_1_3_7_ID:
                return V_1_3_7;
            case V_1_3_6_ID:
                return V_1_3_6;
            case V_1_3_5_ID:
                return V_1_3_5;
            case V_1_3_4_ID:
                return V_1_3_4;
            case V_1_3_3_ID:
                return V_1_3_3;
            case V_1_3_2_ID:
                return V_1_3_2;
            case V_1_3_1_ID:
                return V_1_3_1;
            case V_1_3_0_ID:
                return V_1_3_0;
            case V_1_2_5_ID:
                return V_1_2_5;
            case V_1_2_4_ID:
                return V_1_2_4;
            case V_1_2_3_ID:
                return V_1_2_3;
            case V_1_2_2_ID:
                return V_1_2_2;
            case V_1_2_1_ID:
                return V_1_2_1;
            case V_1_2_0_ID:
                return V_1_2_0;
            case V_1_1_2_ID:
                return V_1_1_2;
            case V_1_1_1_ID:
                return V_1_1_1;
            case V_1_1_0_ID:
                return V_1_1_0;
            case V_1_0_4_ID:
                return V_1_0_4;
            case V_1_0_3_ID:
                return V_1_0_3;
            case V_1_0_2_ID:
                return V_1_0_2;
            case V_1_0_1_ID:
                return V_1_0_1;
            case V_1_0_0_ID:
                return V_1_0_0;
            case V_1_0_0_RC2_ID:
                return V_1_0_0_RC2;
            case V_1_0_0_RC1_ID:
                return V_1_0_0_RC1;
            case V_1_0_0_Beta2_ID:
                return V_1_0_0_Beta2;
            case V_1_0_0_Beta1_ID:
                return V_1_0_0_Beta1;
            case V_0_90_14_ID:
                return V_0_90_14;
            case V_0_90_13_ID:
                return V_0_90_13;
            case V_0_90_12_ID:
                return V_0_90_12;
            case V_0_90_11_ID:
                return V_0_90_11;
            case V_0_90_10_ID:
                return V_0_90_10;
            case V_0_90_9_ID:
                return V_0_90_9;
            case V_0_90_8_ID:
                return V_0_90_8;
            case V_0_90_7_ID:
                return V_0_90_7;
            case V_0_90_6_ID:
                return V_0_90_6;
            case V_0_90_5_ID:
                return V_0_90_5;
            case V_0_90_4_ID:
                return V_0_90_4;
            case V_0_90_3_ID:
                return V_0_90_3;
            case V_0_90_2_ID:
                return V_0_90_2;
            case V_0_90_1_ID:
                return V_0_90_1;
            case V_0_90_0_ID:
                return V_0_90_0;
            case V_0_90_0_RC2_ID:
                return V_0_90_0_RC2;
            case V_0_90_0_RC1_ID:
                return V_0_90_0_RC1;
            case V_0_90_0_Beta1_ID:
                return V_0_90_0_Beta1;            

            default:
                return new Version(id, false, Lucene.VERSION);
        }
    }

    /**
     * Return the {@link Version} of Elasticsearch that has been used to create an index given its settings.
     *
     * @throws ElasticsearchIllegalStateException if the given index settings doesn't contain a value for the key {@value IndexMetaData#SETTING_VERSION_CREATED}
     */
    public static Version indexCreated(Settings indexSettings) {
        final Version indexVersion = indexSettings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null);
        if (indexVersion == null) {
            throw new ElasticsearchIllegalStateException("[" + IndexMetaData.SETTING_VERSION_CREATED + "] is not present in the index settings for index with uuid: [" + indexSettings.get(IndexMetaData.SETTING_UUID) + "]");
        }
        return indexVersion;
    }

    public static void writeVersion(Version version, StreamOutput out) throws IOException {
        out.writeVInt(version.id);
    }

    /**
     * Returns the smallest version between the 2.
     */
    public static Version smallest(Version version1, Version version2) {
        return version1.id < version2.id ? version1 : version2;
    }

    /**
     * Returns the largest version between the 2.
     */
    public static Version largest(Version version1, Version version2) {
        return version1.id > version2.id ? version1 : version2;
    }

    /**
     * Returns the version given its string representation, current version if the argument is null or empty
     */
    public static Version fromString(String version) {
        if (!Strings.hasLength(version)) {
            return Version.CURRENT;
        }
        final boolean snapshot;
        if (snapshot = version.endsWith("-SNAPSHOT")) {
            version = version.substring(0, version.length() - 9);
        }
        String[] parts = version.split("\\.");
        if (parts.length < 3 || parts.length > 4) {
            throw new IllegalArgumentException("the version needs to contain major, minor and revision, and optionally the build");
        }

        try {

            //we reverse the version id calculation based on some assumption as we can't reliably reverse the modulo
            final int major = Integer.parseInt(parts[0]) * 1000000;
            final int minor = Integer.parseInt(parts[1]) * 10000;
            final int revision = Integer.parseInt(parts[2]) * 100;


            int build = 99;
            if (parts.length == 4) {
                String buildStr = parts[3];
                if (buildStr.startsWith("Beta")) {
                    build = Integer.parseInt(buildStr.substring(4));
                }
                if (buildStr.startsWith("RC")) {
                    build = Integer.parseInt(buildStr.substring(2)) + 50;
                }
            }

            final Version versionFromId = fromId(major + minor + revision + build);
            if (snapshot != versionFromId.snapshot()) {
                return new Version(versionFromId.id, snapshot, versionFromId.luceneVersion);
            }
            return versionFromId;

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse version " + version, e);
        }
    }

    public final int id;
    public final byte major;
    public final byte minor;
    public final byte revision;
    public final byte build;
    public final Boolean snapshot;
    public final org.apache.lucene.util.Version luceneVersion;

    Version(int id, boolean snapshot, org.apache.lucene.util.Version luceneVersion) {
        this.id = id;
        this.major = (byte) ((id / 1000000) % 100);
        this.minor = (byte) ((id / 10000) % 100);
        this.revision = (byte) ((id / 100) % 100);
        this.build = (byte) (id % 100);
        this.snapshot = snapshot;
        this.luceneVersion = luceneVersion;
    }

    public boolean snapshot() {
        return snapshot;
    }

    public boolean after(Version version) {
        return version.id < id;
    }

    public boolean onOrAfter(Version version) {
        return version.id <= id;
    }

    public boolean before(Version version) {
        return version.id > id;
    }

    public boolean onOrBefore(Version version) {
        return version.id >= id;
    }

    /**
     * Returns the minimum compatible version based on the current
     * version. Ie a node needs to have at least the return version in order
     * to communicate with a node running the current version. The returned version
     * is in most of the cases the smallest major version release unless the current version
     * is a beta or RC release then the version itself is returned.
     */
    public Version minimumCompatibilityVersion() {
        return Version.smallest(this, fromId(major * 1000000 + 99));
    }

    /**
     * Just the version number (without -SNAPSHOT if snapshot).
     */
    public String number() {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(revision);
        if (build < 50) {
            sb.append(".Beta").append(build);
        } else if (build < 99) {
            sb.append(".RC").append(build - 50);
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println("Version: " + Version.CURRENT + ", Build: " + Build.CURRENT.hashShort() + "/" + Build.CURRENT.timestamp() + ", JVM: " + JvmInfo.jvmInfo().version());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(number());
        if (snapshot()) {
            sb.append("-SNAPSHOT");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Version version = (Version) o;

        if (id != version.id) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }

    public static class Module extends AbstractModule {

        private final Version version;

        public Module(Version version) {
            this.version = version;
        }

        @Override
        protected void configure() {
            bind(Version.class).toInstance(version);
        }
    }
}
