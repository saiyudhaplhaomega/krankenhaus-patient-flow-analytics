/* 1. Drop external tables first */
IF OBJECT_ID('dbo.fact_patient_flow', 'E') IS NOT NULL
    DROP EXTERNAL TABLE dbo.fact_patient_flow;

IF OBJECT_ID('dbo.dim_department', 'E') IS NOT NULL
    DROP EXTERNAL TABLE dbo.dim_department;

IF OBJECT_ID('dbo.dim_patient', 'E') IS NOT NULL
    DROP EXTERNAL TABLE dbo.dim_patient;

/* 2. Drop the external data source */
IF EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'gold_data_source')
    DROP EXTERNAL DATA SOURCE gold_data_source;

/* 3. Drop the credential */
IF EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'storage_credential')
    DROP DATABASE SCOPED CREDENTIAL storage_credential;

/* 4. Drop the master key */
IF EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    DROP MASTER KEY;
