[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ResourceGroup,
    [Parameter(Mandatory = $true)]
    [string]$Location,
    [string]$ParametersFile = "infra/parameters/dev.json",
    [string]$Image = "mcr.microsoft.com/azuredocs/containerapps-helloworld:latest",
    [string]$ApiKeySecretUri = "https://REPLACE_WITH_KEYVAULT.vault.azure.net/secrets/api-key",
    [bool]$DeployWorkloads = $false
)

$ErrorActionPreference = "Stop"
if ($ResourceGroup -ne "rg-derivs-dev") {
    throw "This script refuses non-dev resource groups."
}
if ($DeployWorkloads -and ($Image -match 'REPLACE_WITH|:latest$' -or $Image -notmatch '^[^\s]+:[^\s]+$')) {
    throw "Provide a pushed, immutable ACR image reference including a commit tag/digest."
}
if ($DeployWorkloads -and ($ApiKeySecretUri -match 'REPLACE_WITH' -or $ApiKeySecretUri -notmatch '^https://[^/]+\.vault\.azure\.net/secrets/[^/]+$')) {
    throw "Provide the versionless Key Vault API-key secret URI (never the key value)."
}

& "$PSScriptRoot/dev-what-if.ps1" `
    -ResourceGroup $ResourceGroup `
    -Location $Location `
    -ParametersFile $ParametersFile `
    -Image $Image `
    -DeployWorkloads $DeployWorkloads `
    -ApiKeySecretUri $ApiKeySecretUri
if ($LASTEXITCODE -ne 0) { throw "What-if failed; deployment is cancelled." }

$confirmation = Read-Host "Review what-if output. Type rg-derivs-dev to apply this dev-only deployment"
if ($confirmation -cne "rg-derivs-dev") {
    throw "Confirmation did not match; no resources were deployed."
}
az deployment group create `
    --resource-group $ResourceGroup `
    --template-file infra/main.bicep `
    --parameters "@$ParametersFile" `
    --parameters location=$Location image=$Image deployWorkloads=$DeployWorkloads apiKeySecretUri=$ApiKeySecretUri `
    --mode Incremental
if ($LASTEXITCODE -ne 0) { throw "Deployment failed." }
