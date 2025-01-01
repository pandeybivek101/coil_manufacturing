#!/bin/bash

# Variables for resource group.
RESOURCE_GROUP_NAME="coil_manufacturing"
LOCATION="norwayeast"


# Variables for data lake.
STORAGE_ACCOUNT_NAME="coilmanufacturingstorage"
DATA_REPLICATION="Standard_LRS"   


#variables for databricks access connector.
ACCESS_CONNECTOR_NAME="coil_manufacturing_connector"


#variables for keyvault.
KEY_VAULT_NAME="coil-manufacturing"


#variables for databricks workspace
DATABRICKS_WORKPLACE_NAME="coil_manufacturing_workplace"










######################### START CREATING RESOURCE GROUP #########################################

#If not exist it will create a resource group.
az group create \
  --name "$RESOURCE_GROUP_NAME" \
  --location "$LOCATION"

######################### END CREATING RESOURCE GROUP #########################################










######################### START CREATING DATABRICKS ACCESS CONNECTOR #########################################

#Creating access connector, if not already exists. It acts as an intermediate access provider between Databricks workplace and data lake.
az databricks access-connector create \
    --resource-group "$RESOURCE_GROUP_NAME" \
    --name "$ACCESS_CONNECTOR_NAME" \
    --location "$LOCATION" \
    --identity-type SystemAssigned


#Getting the object ID of the access connector, will be used to assign permission on data lakes.
ACCESS_CONNECTOR_OBJECT_ID=$(az databricks access-connector show \
  --resource-group "$RESOURCE_GROUP_NAME" \
  --name "$ACCESS_CONNECTOR_NAME" \
  --query "identity.principalId" --output tsv)


######################### END CREATING DATABRICKS ACCESS CONNECTOR #########################################










######################### START CREATING DATA LAKE #########################################

#Creating the storage account to store meta-store information, external tables, and volumes.
az storage account create \
  --name "$STORAGE_ACCOUNT_NAME" \
  --resource-group "$RESOURCE_GROUP_NAME" \
  --location "$LOCATION" \
  --sku "$DATA_REPLICATION" \
  --kind StorageV2 \
  --hns true


#Fetching the key, which will be used to create containers.
STORAGE_ACCOUNT_KEY=$(az storage account keys list \
  --resource-group "$RESOURCE_GROUP_NAME" \
  --account-name "$STORAGE_ACCOUNT_NAME" \
  --query '[0].value' \
  --output tsv)



#Creating the required containers within the data lake.
containers=("metastore" "data")
for container in "${containers[@]}"; do
  az storage container create \
    --name "$container" \
    --account-name "$STORAGE_ACCOUNT_NAME" \
    --account-key "$STORAGE_ACCOUNT_KEY"
done



#Creating necessary directories within the containers.
az storage fs directory create \
    --name metadata \
    -f metastore\
    --account-name "$STORAGE_ACCOUNT_NAME"


az storage fs directory create \
    --name volume \
    -f data \
    --account-name "$STORAGE_ACCOUNT_NAME"


az storage fs directory create \
    --name table \
    -f data \
    --account-name "$STORAGE_ACCOUNT_NAME"



#Fetching resource ID, as it is needed for the role assignment to the access connector.
STORAGE_ACCOUNT_RESOURCE_ID=$(az storage account show \
  --name "$STORAGE_ACCOUNT_NAME" \
  --resource-group "$RESOURCE_GROUP_NAME" \
  --query id \
  --output tsv)


#Assigning Data Contributor role to access connector, so that it can read and write into the data lake.
az role assignment create \
  --assignee "$ACCESS_CONNECTOR_OBJECT_ID" \
  --role "Storage Blob Data Contributor" \
  --scope "$STORAGE_ACCOUNT_RESOURCE_ID"


######################### END CREATING DATA LAKE #########################################










######################### START CREATING KEY VAULT #########################################

#Creating the azure key vault, if it does not exist.
if ! az keyvault show --name "$KEY_VAULT_NAME" --resource-group "$RESOURCE_GROUP_NAME" &>/dev/null; then
    az keyvault create \
        --location "$LOCATION" \
        --name "$KEY_VAULT_NAME" \
        --enable-rbac-authorization false \
        --resource-group "$RESOURCE_GROUP_NAME"
fi


#Fetching the Databricks application object ID, the Databricks application needs to have get access on the secrets to read it from the workplace.
DATABRICKS_APPLICATION_OBJECT_ID=$(az ad sp list \
    --filter "displayName eq 'AzureDatabricks'" \
    --query '[0].id' -o tsv)


#Setting up access policy.
az keyvault set-policy \
    --name "$KEY_VAULT_NAME" \
    --secret-permissions get list \
    --object-id "$DATABRICKS_APPLICATION_OBJECT_ID"


#Storing the absolute path of the root URL holding the volume and external tables.
ROOT_PATH="abfss://data@$STORAGE_ACCOUNT_NAME.dfs.core.windows.net/"
az keyvault secret set \
    --vault-name "$KEY_VAULT_NAME" \
    --name "rootPath" \
    --value "$ROOT_PATH"

######################### END CREATING KEY VAULT #########################################










######################### START CREATING DATABRICKS WORKSPACE ########################################

#Creating the Databricks workspace.
az databricks workspace create \
    --resource-group "$RESOURCE_GROUP_NAME" \
    --name "$DATABRICKS_WORKPLACE_NAME" \
    --location "$LOCATION"  \
    --sku premium

######################### END CREATING DATABRICKS WORKSPACE  #########################################



