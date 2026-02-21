package com.danubemessaging.client;

import com.danubemessaging.client.errors.DanubeClientException;
import com.danubemessaging.client.internal.auth.AuthService;
import com.danubemessaging.client.internal.connection.ConnectionManager;
import com.danubemessaging.client.schema.CompatibilityCheck;
import com.danubemessaging.client.schema.CompatibilityMode;
import com.danubemessaging.client.schema.OperationResult;
import com.danubemessaging.client.schema.SchemaInfo;
import com.danubemessaging.client.schema.SchemaRegistration;
import com.danubemessaging.client.schema.SchemaRegistrationBuilder;
import com.danubemessaging.client.schema.SchemaRegistrationRequest;
import com.danubemessaging.client.schema.SchemaType;
import com.danubemessaging.client.schema.SchemaVersionInfo;
import com.danubemessaging.client.schema.TopicSchemaConfig;
import com.danubemessaging.client.schema.ValidationPolicy;
import danube_schema.SchemaRegistryGrpc;
import danube_schema.SchemaRegistryOuterClass;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Client for Danube schema registry operations.
 */
public final class SchemaRegistryClient {
    private final URI serviceUri;
    private final ConnectionManager connectionManager;
    private final AuthService authService;
    private final ExecutorService ioExecutor;

    SchemaRegistryClient(
            URI serviceUri,
            ConnectionManager connectionManager,
            AuthService authService,
            ExecutorService ioExecutor) {
        this.serviceUri = Objects.requireNonNull(serviceUri, "serviceUri");
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager");
        this.authService = Objects.requireNonNull(authService, "authService");
        this.ioExecutor = Objects.requireNonNull(ioExecutor, "ioExecutor");
    }

    public SchemaRegistrationBuilder newRegistration() {
        return new SchemaRegistrationBuilder();
    }

    public CompletableFuture<SchemaRegistration> registerSchemaAsync(SchemaRegistrationRequest request) {
        return CompletableFuture.supplyAsync(() -> registerSchema(request), ioExecutor);
    }

    public CompletableFuture<SchemaRegistration> registerSchemaAsync(SchemaRegistrationBuilder builder) {
        Objects.requireNonNull(builder, "builder");
        return registerSchemaAsync(builder.build());
    }

    public SchemaRegistration registerSchema(SchemaRegistrationRequest request) {
        Objects.requireNonNull(request, "request");

        SchemaRegistryOuterClass.RegisterSchemaRequest protoRequest = SchemaRegistryOuterClass.RegisterSchemaRequest
                .newBuilder()
                .setSubject(request.subject())
                .setSchemaType(request.schemaType().toWireValue())
                .setSchemaDefinition(com.google.protobuf.ByteString.copyFrom(request.schemaDefinition()))
                .setDescription(request.description())
                .setCreatedBy(request.createdBy())
                .addAllTags(request.tags())
                .build();

        try {
            SchemaRegistryOuterClass.RegisterSchemaResponse response = stub().registerSchema(protoRequest);
            return new SchemaRegistration(
                    response.getSchemaId(),
                    response.getVersion(),
                    response.getIsNewVersion(),
                    response.getFingerprint());
        } catch (Exception error) {
            throw new DanubeClientException("Failed to register schema for subject: " + request.subject(), error);
        }
    }

    public SchemaRegistration registerSchema(SchemaRegistrationBuilder builder) {
        Objects.requireNonNull(builder, "builder");
        return registerSchema(builder.build());
    }

    public CompletableFuture<OperationResult> configureTopicSchemaAsync(
            String topicName,
            String schemaSubject,
            ValidationPolicy validationPolicy,
            boolean enablePayloadValidation) {
        return CompletableFuture.supplyAsync(
                () -> configureTopicSchema(topicName, schemaSubject, validationPolicy, enablePayloadValidation),
                ioExecutor);
    }

    public CompletableFuture<OperationResult> setCompatibilityModeAsync(String subject, CompatibilityMode mode) {
        return CompletableFuture.supplyAsync(() -> setCompatibilityMode(subject, mode), ioExecutor);
    }

    public CompletableFuture<SchemaInfo> getSchemaByIdAsync(long schemaId) {
        return CompletableFuture.supplyAsync(() -> getSchemaById(schemaId), ioExecutor);
    }

    public SchemaInfo getSchemaById(long schemaId) {
        return getSchemaById(schemaId, null);
    }

    public CompletableFuture<SchemaInfo> getSchemaByIdAsync(long schemaId, Integer version) {
        return CompletableFuture.supplyAsync(() -> getSchemaById(schemaId, version), ioExecutor);
    }

    public SchemaInfo getSchemaById(long schemaId, Integer version) {
        if (schemaId <= 0) {
            throw new DanubeClientException("Schema id must be greater than zero");
        }

        SchemaRegistryOuterClass.GetSchemaRequest.Builder requestBuilder = SchemaRegistryOuterClass.GetSchemaRequest
                .newBuilder()
                .setSchemaId(schemaId);

        if (version != null) {
            if (version <= 0) {
                throw new DanubeClientException("Schema version must be greater than zero");
            }
            requestBuilder.setVersion(version);
        }

        try {
            SchemaRegistryOuterClass.GetSchemaResponse response = stub().getSchema(requestBuilder.build());
            return toSchemaInfo(response);
        } catch (Exception error) {
            throw new DanubeClientException("Failed to fetch schema by id: " + schemaId, error);
        }
    }

    public CompletableFuture<SchemaInfo> getLatestSchemaAsync(String subject) {
        return CompletableFuture.supplyAsync(() -> getLatestSchema(subject), ioExecutor);
    }

    public SchemaInfo getLatestSchema(String subject) {
        String value = requireText(subject, "Schema subject is required");

        SchemaRegistryOuterClass.GetLatestSchemaRequest request = SchemaRegistryOuterClass.GetLatestSchemaRequest
                .newBuilder()
                .setSubject(value)
                .build();

        try {
            SchemaRegistryOuterClass.GetSchemaResponse response = stub().getLatestSchema(request);
            return toSchemaInfo(response);
        } catch (Exception error) {
            throw new DanubeClientException("Failed to fetch latest schema for subject: " + value, error);
        }
    }

    public CompletableFuture<List<SchemaVersionInfo>> listVersionsAsync(String subject) {
        return CompletableFuture.supplyAsync(() -> listVersions(subject), ioExecutor);
    }

    public List<SchemaVersionInfo> listVersions(String subject) {
        String value = requireText(subject, "Schema subject is required");

        SchemaRegistryOuterClass.ListVersionsRequest request = SchemaRegistryOuterClass.ListVersionsRequest.newBuilder()
                .setSubject(value)
                .build();

        try {
            SchemaRegistryOuterClass.ListVersionsResponse response = stub().listVersions(request);
            List<SchemaVersionInfo> versions = new ArrayList<>(response.getVersionsCount());
            for (SchemaRegistryOuterClass.SchemaVersionInfo versionInfo : response.getVersionsList()) {
                versions.add(new SchemaVersionInfo(
                        versionInfo.getVersion(),
                        versionInfo.getCreatedAt(),
                        versionInfo.getCreatedBy(),
                        versionInfo.getDescription(),
                        versionInfo.getFingerprint(),
                        versionInfo.getSchemaId()));
            }
            return versions;
        } catch (Exception error) {
            throw new DanubeClientException("Failed to list schema versions for subject: " + value, error);
        }
    }

    public CompatibilityCheck checkCompatibility(
            String subject,
            byte[] schemaDefinition,
            SchemaType schemaType,
            CompatibilityMode overrideMode) {
        String subjectValue = requireText(subject, "Schema subject is required");
        if (schemaType == null) {
            throw new DanubeClientException("Schema type is required");
        }

        byte[] payload = schemaDefinition == null ? new byte[0] : schemaDefinition.clone();
        if (payload.length == 0) {
            throw new DanubeClientException("Schema definition is required");
        }

        SchemaRegistryOuterClass.CheckCompatibilityRequest.Builder requestBuilder = SchemaRegistryOuterClass.CheckCompatibilityRequest
                .newBuilder()
                .setSubject(subjectValue)
                .setSchemaType(schemaType.toWireValue())
                .setNewSchemaDefinition(com.google.protobuf.ByteString.copyFrom(payload));

        if (overrideMode != null) {
            requestBuilder.setCompatibilityMode(overrideMode.toWireValue());
        }

        try {
            SchemaRegistryOuterClass.CheckCompatibilityResponse response = stub()
                    .checkCompatibility(requestBuilder.build());
            return new CompatibilityCheck(response.getIsCompatible(), response.getErrorsList());
        } catch (Exception error) {
            throw new DanubeClientException("Failed to check compatibility for subject: " + subjectValue, error);
        }
    }

    public CompletableFuture<CompatibilityCheck> checkCompatibilityAsync(
            String subject,
            byte[] schemaDefinition,
            SchemaType schemaType,
            CompatibilityMode overrideMode) {
        return CompletableFuture.supplyAsync(
                () -> checkCompatibility(subject, schemaDefinition, schemaType, overrideMode),
                ioExecutor);
    }

    public CompatibilityCheck checkCompatibility(String subject, byte[] schemaDefinition, SchemaType schemaType) {
        return checkCompatibility(subject, schemaDefinition, schemaType, null);
    }

    public CompletableFuture<CompatibilityCheck> checkCompatibilityAsync(
            String subject,
            byte[] schemaDefinition,
            SchemaType schemaType) {
        return checkCompatibilityAsync(subject, schemaDefinition, schemaType, null);
    }

    public OperationResult deleteSchemaVersion(String subject, int version) {
        String subjectValue = requireText(subject, "Schema subject is required");
        if (version <= 0) {
            throw new DanubeClientException("Schema version must be greater than zero");
        }

        SchemaRegistryOuterClass.DeleteSchemaVersionRequest request = SchemaRegistryOuterClass.DeleteSchemaVersionRequest
                .newBuilder()
                .setSubject(subjectValue)
                .setVersion(version)
                .build();

        try {
            SchemaRegistryOuterClass.DeleteSchemaVersionResponse response = stub().deleteSchemaVersion(request);
            return new OperationResult(response.getSuccess(), response.getMessage());
        } catch (Exception error) {
            throw new DanubeClientException(
                    "Failed to delete schema version " + version + " for subject: " + subjectValue,
                    error);
        }
    }

    public CompletableFuture<OperationResult> deleteSchemaVersionAsync(String subject, int version) {
        return CompletableFuture.supplyAsync(() -> deleteSchemaVersion(subject, version), ioExecutor);
    }

    public OperationResult setCompatibilityMode(String subject, CompatibilityMode mode) {
        String subjectValue = requireText(subject, "Schema subject is required");
        if (mode == null) {
            throw new DanubeClientException("Compatibility mode is required");
        }

        SchemaRegistryOuterClass.SetCompatibilityModeRequest request = SchemaRegistryOuterClass.SetCompatibilityModeRequest
                .newBuilder()
                .setSubject(subjectValue)
                .setCompatibilityMode(mode.toWireValue())
                .build();

        try {
            SchemaRegistryOuterClass.SetCompatibilityModeResponse response = stub().setCompatibilityMode(request);
            return new OperationResult(response.getSuccess(), response.getMessage());
        } catch (Exception error) {
            throw new DanubeClientException("Failed to set compatibility mode for subject: " + subjectValue, error);
        }
    }

    public OperationResult configureTopicSchema(
            String topicName,
            String schemaSubject,
            ValidationPolicy validationPolicy,
            boolean enablePayloadValidation) {
        String topic = requireText(topicName, "Topic name is required");
        String subject = requireText(schemaSubject, "Schema subject is required");
        ValidationPolicy policy = validationPolicy == null ? ValidationPolicy.NONE : validationPolicy;

        SchemaRegistryOuterClass.ConfigureTopicSchemaRequest request = SchemaRegistryOuterClass.ConfigureTopicSchemaRequest
                .newBuilder()
                .setTopicName(topic)
                .setSchemaSubject(subject)
                .setValidationPolicy(policy.toWireValue())
                .setEnablePayloadValidation(enablePayloadValidation)
                .build();

        try {
            SchemaRegistryOuterClass.ConfigureTopicSchemaResponse response = stub().configureTopicSchema(request);
            return new OperationResult(response.getSuccess(), response.getMessage());
        } catch (Exception error) {
            throw new DanubeClientException("Failed to configure topic schema for topic: " + topic, error);
        }
    }

    public OperationResult updateTopicValidationPolicy(
            String topicName,
            ValidationPolicy validationPolicy,
            boolean enablePayloadValidation) {
        String topic = requireText(topicName, "Topic name is required");
        ValidationPolicy policy = validationPolicy == null ? ValidationPolicy.NONE : validationPolicy;

        SchemaRegistryOuterClass.UpdateTopicValidationPolicyRequest request = SchemaRegistryOuterClass.UpdateTopicValidationPolicyRequest
                .newBuilder()
                .setTopicName(topic)
                .setValidationPolicy(policy.toWireValue())
                .setEnablePayloadValidation(enablePayloadValidation)
                .build();

        try {
            SchemaRegistryOuterClass.UpdateTopicValidationPolicyResponse response = stub()
                    .updateTopicValidationPolicy(request);
            return new OperationResult(response.getSuccess(), response.getMessage());
        } catch (Exception error) {
            throw new DanubeClientException("Failed to update topic validation policy for topic: " + topic, error);
        }
    }

    public CompletableFuture<OperationResult> updateTopicValidationPolicyAsync(
            String topicName,
            ValidationPolicy validationPolicy,
            boolean enablePayloadValidation) {
        return CompletableFuture.supplyAsync(
                () -> updateTopicValidationPolicy(topicName, validationPolicy, enablePayloadValidation),
                ioExecutor);
    }

    public TopicSchemaConfig getTopicSchemaConfig(String topicName) {
        String topic = requireText(topicName, "Topic name is required");

        SchemaRegistryOuterClass.GetTopicSchemaConfigRequest request = SchemaRegistryOuterClass.GetTopicSchemaConfigRequest
                .newBuilder()
                .setTopicName(topic)
                .build();

        try {
            SchemaRegistryOuterClass.GetTopicSchemaConfigResponse response = stub().getTopicSchemaConfig(request);
            return new TopicSchemaConfig(
                    response.getSchemaSubject(),
                    ValidationPolicy.fromWireValue(response.getValidationPolicy()),
                    response.getEnablePayloadValidation(),
                    response.getSchemaId());
        } catch (Exception error) {
            throw new DanubeClientException("Failed to fetch topic schema config for topic: " + topic, error);
        }
    }

    public CompletableFuture<TopicSchemaConfig> getTopicSchemaConfigAsync(String topicName) {
        return CompletableFuture.supplyAsync(() -> getTopicSchemaConfig(topicName), ioExecutor);
    }

    private SchemaRegistryGrpc.SchemaRegistryBlockingStub stub() {
        var grpcConnection = connectionManager.getConnection(serviceUri, serviceUri);
        return authService.attachAuthIfNeeded(
                SchemaRegistryGrpc.newBlockingStub(grpcConnection.grpcChannel()),
                serviceUri);
    }

    private static SchemaInfo toSchemaInfo(SchemaRegistryOuterClass.GetSchemaResponse response) {
        return new SchemaInfo(
                response.getSchemaId(),
                response.getVersion(),
                response.getSubject(),
                SchemaType.fromWireValue(response.getSchemaType()),
                response.getSchemaDefinition().toByteArray(),
                response.getDescription(),
                response.getCreatedAt(),
                response.getCreatedBy(),
                response.getTagsList(),
                response.getFingerprint(),
                CompatibilityMode.fromWireValue(response.getCompatibilityMode()));
    }

    private static String requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new DanubeClientException(message);
        }
        return value;
    }
}
