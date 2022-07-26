# databricks-athena-blog

Source code to automate the Databricks to Athena manifest based integration as described here

https://docs.databricks.com/delta/presto-integration.html

The main API is in class `DeltaIntegration` 

During the course of automation the following steps are performed
- Manifest is created
- Auto-update manifest is set to true on Delta table
- Create external table statement is generated
- New table is created in Athena using AWS SDK
- `msck repair table` is run on new Athena table  
- Validation query is executed to verify new table is queryable