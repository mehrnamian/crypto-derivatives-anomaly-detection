targetScope = 'resourceGroup'

@description('Deployment location; choose a supported European region after the exchange connectivity test.')
param location string = resourceGroup().location
@description('Environment suffix, normally dev. Prod is intentionally not configured here.')
@allowed([
  'dev'
])
param environment string = 'dev'
@description('Globally unique storage account name (3-24 lowercase letters/numbers).')
@minLength(3)
@maxLength(24)
param storageAccountName string
@description('Globally unique Container Registry name (5-50 alphanumeric characters).')
@minLength(5)
@maxLength(50)
param registryName string
@description('Globally unique Event Hubs namespace name.')
@minLength(6)
@maxLength(50)
param eventHubNamespaceName string
@description('Globally unique Key Vault name. Create the API key secret before deployment.')
@minLength(3)
@maxLength(24)
param keyVaultName string
@description('Versionless Key Vault secret URI for the API key; no secret value is stored in this template.')
param apiKeySecretUri string
@description('Container image to run. Build/push this image before starting the app/job.')
param image string
@description('Set true only after image is pushed to ACR and the API secret exists in Key Vault.')
param deployWorkloads bool = false
@description('Cost ownership tag. Set a non-empty project value.')
param projectTag string = 'derivs-anomaly'

var prefix = 'derivs-${environment}'
var eventHubFqdn = '${eventHubNamespaceName}.servicebus.windows.net'
var captureFormat = '{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}'

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${prefix}-logs'
  location: location
  tags: { project: projectTag, environment: environment }
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
    features: { enableLogAccessUsingOnlyResourcePermissions: true }
  }
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: { project: projectTag, environment: environment }
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    publicNetworkAccess: 'Enabled'
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = [for name in [
  'bronze'
  'silver'
  'gold'
  'quarantine'
]: {
  parent: blobService
  name: name
  properties: { publicAccess: 'None' }
}]

resource eventNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: eventHubNamespaceName
  location: location
  tags: { project: projectTag, environment: environment }
  sku: { name: 'Standard', tier: 'Standard', capacity: 1 }
  properties: {
    minimumTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
    disableLocalAuth: true
  }
  identity: { type: 'SystemAssigned' }
}

resource captureHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: eventNamespace
  name: 'market-events'
  properties: {
    messageRetentionInDays: 1
    partitionCount: 2
    status: 'Active'
    captureDescription: {
      enabled: true
      encoding: 'Avro'
      intervalInSeconds: 300
      sizeLimitInBytes: 314572800
      skipEmptyArchives: true
      destination: {
        name: 'EventHubArchive.AzureBlockBlob'
        properties: {
          storageAccountResourceId: storage.id
          blobContainer: 'bronze'
          archiveNameFormat: '${captureFormat}.avro'
        }
      }
    }
  }
}

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: registryName
  location: location
  tags: { project: projectTag, environment: environment }
  sku: { name: 'Basic' }
  properties: {
    adminUserEnabled: false
    publicNetworkAccess: 'Enabled'
  }
}

resource vault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: { project: projectTag, environment: environment }
  properties: {
    tenantId: tenant().tenantId
    sku: { family: 'A', name: 'standard' }
    enableRbacAuthorization: true
    enablePurgeProtection: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    publicNetworkAccess: 'Enabled'
    accessPolicies: []
  }
}

resource ingestIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${prefix}-ingest-mi'
  location: location
  tags: { project: projectTag, environment: environment }
}

resource processIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${prefix}-process-mi'
  location: location
  tags: { project: projectTag, environment: environment }
}

resource apiIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${prefix}-api-mi'
  location: location
  tags: { project: projectTag, environment: environment }
}

resource backfillIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${prefix}-backfill-mi'
  location: location
  tags: { project: projectTag, environment: environment }
}

resource appEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: '${prefix}-aca'
  location: location
  tags: { project: projectTag, environment: environment }
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
    workloadProfiles: [
      { name: 'Consumption', workloadProfileType: 'Consumption' }
    ]
  }
}

resource ingestApp 'Microsoft.App/containerApps@2024-03-01' = if (deployWorkloads) {
  name: '${prefix}-ingest'
  location: location
  tags: { project: projectTag, environment: environment }
  identity: { type: 'UserAssigned', userAssignedIdentities: { '${ingestIdentity.id}': {} } }
  properties: {
    managedEnvironmentId: appEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      registries: [
        { server: registry.properties.loginServer, identity: ingestIdentity.id }
      ]
    }
    template: {
      revisionSuffix: 'dev'
      containers: [
        {
          name: 'ingest'
          image: image
          command: [ 'derivs-anomaly' ]
          args: [ 'ingest' ]
          env: [
            { name: 'DERIVS_EXCHANGE', value: 'binance' }
            { name: 'DERIVS_SYMBOL', value: 'BTCUSDT' }
            { name: 'DERIVS_EVENTHUB_NAMESPACE', value: eventHubFqdn }
            { name: 'DERIVS_EVENTHUB_NAME', value: captureHub.name }
            { name: 'DERIVS_DATA_DIR', value: '/data' }
          ]
          resources: { cpu: 0.5, memory: '1Gi' }
          volumeMounts: [ { volumeName: 'data', mountPath: '/data' } ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
      volumes: [ { name: 'data', storageType: 'EmptyDir' } ]
    }
  }
  dependsOn: [registryIngestRole, senderRole]
}

resource processJob 'Microsoft.App/jobs@2024-03-01' = if (deployWorkloads) {
  name: '${prefix}-process'
  location: location
  tags: { project: projectTag, environment: environment }
  identity: { type: 'UserAssigned', userAssignedIdentities: { '${processIdentity.id}': {} } }
  properties: {
    environmentId: appEnvironment.id
    configuration: {
      triggerType: 'Schedule'
      replicaTimeout: 1800
      replicaRetryLimit: 2
      scheduleTriggerConfig: {
        cronExpression: '*/5 * * * *'
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [ { server: registry.properties.loginServer, identity: processIdentity.id } ]
    }
    template: {
      containers: [
        {
          name: 'process'
          image: image
          command: [ 'derivs-anomaly' ]
          args: [ 'process' ]
          env: [
            { name: 'DERIVS_STORAGE_ACCOUNT_URL', value: 'https://${storage.name}.dfs.core.windows.net' }
            { name: 'DERIVS_EVENTHUB_NAMESPACE', value: eventHubFqdn }
            { name: 'DERIVS_EVENTHUB_NAME', value: captureHub.name }
            { name: 'DERIVS_DATA_DIR', value: '/data' }
          ]
          resources: { cpu: 1, memory: '2Gi' }
        }
      ]
    }
  }
  dependsOn: [registryJobRole, jobBronzeReadRole, jobSilverWriteRole, jobGoldWriteRole, jobQuarantineWriteRole]
}

resource apiApp 'Microsoft.App/containerApps@2024-03-01' = if (deployWorkloads) {
  name: '${prefix}-api'
  location: location
  tags: { project: projectTag, environment: environment }
  identity: { type: 'UserAssigned', userAssignedIdentities: { '${apiIdentity.id}': {} } }
  properties: {
    managedEnvironmentId: appEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      registries: [ { server: registry.properties.loginServer, identity: apiIdentity.id } ]
      secrets: [
        {
          name: 'api-key'
          keyVaultUrl: apiKeySecretUri
          identity: apiIdentity.id
        }
      ]
      ingress: {
        external: true
        targetPort: 8000
        transport: 'http'
        allowInsecure: false
        traffic: [ { latestRevision: true, weight: 100 } ]
      }
    }
    template: {
      containers: [
        {
          name: 'api'
          image: image
          command: [ 'derivs-anomaly' ]
          args: [ 'api' ]
          env: [
            { name: 'DERIVS_API_HOST', value: '0.0.0.0' }
            { name: 'DERIVS_API_KEY', secretRef: 'api-key' }
            { name: 'DERIVS_DATA_DIR', value: '/data' }
            { name: 'DERIVS_STORAGE_ACCOUNT_URL', value: 'https://${storage.name}.dfs.core.windows.net' }
            { name: 'DERIVS_MODEL_VERSION', value: 'robust-zscore-v1' }
          ]
          resources: { cpu: 0.5, memory: '1Gi' }
          volumeMounts: [ { volumeName: 'data', mountPath: '/data' } ]
        }
      ]
      scale: { minReplicas: 1, maxReplicas: 1 }
      volumes: [ { name: 'data', storageType: 'EmptyDir' } ]
    }
  }
  dependsOn: [registryApiRole, apiKeyVaultRole, apiGoldReadRole]
}

resource backfillJob 'Microsoft.App/jobs@2024-03-01' = if (deployWorkloads) {
  name: '${prefix}-backfill'
  location: location
  tags: { project: projectTag, environment: environment }
  identity: { type: 'UserAssigned', userAssignedIdentities: { '${backfillIdentity.id}': {} } }
  properties: {
    environmentId: appEnvironment.id
    configuration: {
      triggerType: 'Schedule'
      replicaTimeout: 1800
      replicaRetryLimit: 2
      scheduleTriggerConfig: {
        cronExpression: '*/5 * * * *'
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [ { server: registry.properties.loginServer, identity: backfillIdentity.id } ]
    }
    template: {
      containers: [
        {
          name: 'backfill'
          image: image
          command: [ 'derivs-anomaly' ]
          args: [ 'recover' ]
          env: [
            { name: 'DERIVS_STORAGE_ACCOUNT_URL', value: 'https://${storage.name}.dfs.core.windows.net' }
            { name: 'DERIVS_EVENTHUB_NAMESPACE', value: eventHubFqdn }
            { name: 'DERIVS_EVENTHUB_NAME', value: captureHub.name }
            { name: 'DERIVS_DATA_DIR', value: '/data' }
          ]
          resources: { cpu: 0.5, memory: '1Gi' }
        }
      ]
    }
  }
  dependsOn: [registryBackfillRole, backfillEventHubSenderRole, backfillGoldReadRole]
}

resource qualityJob 'Microsoft.App/jobs@2024-03-01' = if (deployWorkloads) {
  name: '${prefix}-quality'
  location: location
  tags: { project: projectTag, environment: environment }
  identity: { type: 'UserAssigned', userAssignedIdentities: { '${backfillIdentity.id}': {} } }
  properties: {
    environmentId: appEnvironment.id
    configuration: {
      triggerType: 'Schedule'
      replicaTimeout: 900
      replicaRetryLimit: 1
      scheduleTriggerConfig: {
        cronExpression: '10 0 * * *'
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [ { server: registry.properties.loginServer, identity: backfillIdentity.id } ]
    }
    template: {
      containers: [
        {
          name: 'quality'
          image: image
          command: [ 'derivs-anomaly' ]
          args: [ 'quality' ]
          env: [
            { name: 'DERIVS_STORAGE_ACCOUNT_URL', value: 'https://${storage.name}.dfs.core.windows.net' }
            { name: 'DERIVS_DATA_DIR', value: '/data' }
          ]
          resources: { cpu: 0.25, memory: '0.5Gi' }
        }
      ]
    }
  }
  dependsOn: [registryBackfillRole, backfillGoldReadRole]
}

resource storageCaptureRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(containers[0].id, eventNamespace.id, 'capture-writer')
  scope: containers[0]
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: eventNamespace.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource senderRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(captureHub.id, ingestIdentity.id, 'eventhub-sender')
  scope: captureHub
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2b629674-e913-4c01-ae53-ef4638d8f975')
    principalId: ingestIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource registryIngestRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, ingestIdentity.id, 'ingest-acrpull')
  scope: registry
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: ingestIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource registryJobRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, processIdentity.id, 'job-acrpull')
  scope: registry
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: processIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource registryApiRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, apiIdentity.id, 'api-acrpull')
  scope: registry
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: apiIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource apiKeyVaultRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(vault.id, apiIdentity.id, 'keyvault-secrets-user')
  scope: vault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: apiIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource apiGoldReadRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(containers[2].id, apiIdentity.id, 'gold-reader')
  scope: containers[2]
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: apiApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource jobBronzeReadRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(containers[0].id, processIdentity.id, 'bronze-reader')
  scope: containers[0]
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: processIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource jobSilverWriteRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(containers[1].id, processIdentity.id, 'silver-writer')
  scope: containers[1]
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: processIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource jobGoldWriteRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(containers[2].id, processIdentity.id, 'gold-writer')
  scope: containers[2]
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: processIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource jobQuarantineWriteRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(containers[3].id, processIdentity.id, 'quarantine-writer')
  scope: containers[3]
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: processIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource registryBackfillRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, backfillIdentity.id, 'backfill-acrpull')
  scope: registry
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: backfillIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource backfillEventHubSenderRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(captureHub.id, backfillIdentity.id, 'backfill-eventhub-sender')
  scope: captureHub
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2b629674-e913-4c01-ae53-ef4638d8f975')
    principalId: backfillIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource backfillGoldReadRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(containers[2].id, backfillIdentity.id, 'backfill-gold-reader')
  scope: containers[2]
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: backfillIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

output storageEndpoint string = 'https://${storage.name}.dfs.core.windows.net'
output eventHubNamespaceFqdn string = eventHubFqdn
output eventHubName string = captureHub.name
output registryLoginServer string = registry.properties.loginServer
output logAnalyticsWorkspaceId string = logAnalytics.id
output apiUrl string = deployWorkloads ? 'https://${apiApp!.properties.configuration.ingress.fqdn}' : 'not-deployed'
output keyVaultUri string = vault.properties.vaultUri
