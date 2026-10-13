[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ResourceGroup,
    [Parameter(Mandatory = $true)]
    [string]$Location,
    [string]$ParametersFile = "infra/parameters/dev.json",
    [string]$Image = "mcr.microsoft.com/azuredocs/containerapps-helloworld:latest",
    [bool]$DeployWorkloads = $false,
    [string]$ApiKeySecretUri = "https://REPLACE_WITH_KEYVAULT.vault.azure.net/secrets/api-key"
)

$ErrorActionPreference = "Stop"
if ($ResourceGroup -notmatch '^rg-derivs-dev$') {
    throw "Only rg-derivs-dev is allowed by this dev-only helper."
}
if ($Location -notmatch '^(northeurope|westeurope|swedencentral|francecentral|germanywestcentral|uksouth|italynorth|spaincentral|polandcentral|switzerlandnorth|norwayeast|belgiumcentral|denmarkeast|finlandcentral|austriaeast|greececentral)$') {
    throw "Choose a supported European region, then verify exchange reachability before any deployment."
}
if (-not (Test-Path $ParametersFile)) {
    throw "Parameters file not found: $ParametersFile"
}

az account show --output none
if ($LASTEXITCODE -ne 0) { throw "Azure CLI is not logged in." }
az group show --name $ResourceGroup --output none
if ($LASTEXITCODE -ne 0) { throw "Create/select the dev resource group and budget before running what-if." }

az bicep build --file infra/main.bicep --outfile "$env:TEMP/derivs-main.json"
if ($LASTEXITCODE -ne 0) { throw "Bicep build failed." }

az deployment group what-if `
    --resource-group $ResourceGroup `
    --template-file infra/main.bicep `
    --parameters "@$ParametersFile" `
    --parameters location=$Location image=$Image deployWorkloads=$DeployWorkloads apiKeySecretUri=$ApiKeySecretUri `
    --result-format FullResourcePayloads
if ($LASTEXITCODE -ne 0) { throw "Azure what-if failed." }
