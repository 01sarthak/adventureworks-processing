# adventureworks-processing
This repo is about databricks implementation of processing files generated from adventureworks db in medallion architecture

Below are the steps for Creating Azure Key Vault backed Secret scope, then granting Azure Databricks access to Azure Keyvault service so that it can fetch the necessary keys

## Step1
Go to https://**databricks-instance-name**#secrets/createScope. This URL is case sensitive; scope in createScope must be uppercase.

![image](https://github.com/01sarthak/adventureworks-processing/assets/43268414/c61810e9-0ce6-409c-a1a8-d8ea453c8323)
Enter the name of the secret scope. Secret scope names are case insensitive.
Use the Manage Principal drop-down to specify whether All Users have MANAGE permission for this secret scope or only the Creator of the secret scope (that is to say, you).
## Step2
Get DNS Names and Resource ID details  from properties tab of Azure Key Vault and update respective fields.

### Next set of steps define of how to grant access to databricks application to Azure Key vault

## Step1
![image](https://github.com/01sarthak/adventureworks-processing/assets/43268414/ce695618-910a-4a60-be7d-3507bb1d4d13)
## step2
![image](https://github.com/01sarthak/adventureworks-processing/assets/43268414/dfd03d74-418e-4dca-90c9-cd09fa281e85)



