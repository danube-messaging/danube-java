package com.danubemessaging.client.schema;

import com.danubemessaging.client.errors.DanubeClientException;
import danube.DanubeApi;

/**
 * Producer schema reference selection for topic publish operations.
 */
public record SchemaReference(
        String subject,
        boolean useLatest,
        Integer pinnedVersion,
        Integer minVersion) {

    public SchemaReference {
        if (subject == null || subject.isBlank()) {
            throw new DanubeClientException("Schema reference subject is required");
        }

        int selected = (useLatest ? 1 : 0) + (pinnedVersion != null ? 1 : 0) + (minVersion != null ? 1 : 0);
        if (selected != 1) {
            throw new DanubeClientException("Schema reference must select exactly one version strategy");
        }

        if (pinnedVersion != null && pinnedVersion <= 0) {
            throw new DanubeClientException("Pinned schema version must be greater than zero");
        }

        if (minVersion != null && minVersion <= 0) {
            throw new DanubeClientException("Minimum schema version must be greater than zero");
        }
    }

    public static SchemaReference latest(String subject) {
        return new SchemaReference(subject, true, null, null);
    }

    public static SchemaReference pinnedVersion(String subject, int version) {
        return new SchemaReference(subject, false, version, null);
    }

    public static SchemaReference minVersion(String subject, int version) {
        return new SchemaReference(subject, false, null, version);
    }

    public DanubeApi.SchemaReference toProto() {
        DanubeApi.SchemaReference.Builder builder = DanubeApi.SchemaReference.newBuilder().setSubject(subject);

        if (useLatest) {
            builder.setUseLatest(true);
        } else if (pinnedVersion != null) {
            builder.setPinnedVersion(pinnedVersion);
        } else {
            builder.setMinVersion(minVersion);
        }

        return builder.build();
    }
}
