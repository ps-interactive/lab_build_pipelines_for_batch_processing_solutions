--
-- Schemas - active & stage
--

-- Drop schema
DROP SCHEMA IF EXISTS [active]
GO
 
-- Add schema
CREATE SCHEMA [active] AUTHORIZATION [dbo]
GO

-- Drop schema
DROP SCHEMA IF EXISTS [stage]
GO
 
-- Add schema
CREATE SCHEMA [stage] AUTHORIZATION [dbo]
GO

--
-- Table - staged customer acquisition data
--

-- Drop stage table
DROP TABLE IF EXISTS [stage].[customer_acquistion_data]
GO

-- Create stage table
CREATE TABLE [stage].[customer_acquistion_data]
(
	[branch_id] varchar(64) null,
	[relationship_manager_id] varchar(64) null,
	[date] varchar(64) null,
	[customer_count] varchar(64) null
) 
GO

--
-- Table - current watermark
--

-- Drop stage table
DROP TABLE IF EXISTS [stage].[current_watermark]
GO

-- Create stage table
CREATE TABLE [stage].[current_watermark]
(
	[process_date_hour] datetime not null
)
GO

-- Clear any existing data
TRUNCATE TABLE [stage].[current_watermark]
GO

-- Add data to control table
INSERT INTO [stage].[current_watermark] VALUES ('20210101 00:00:00')
GO

--
-- Table - active customer acquisition data
--

-- Drop active table
DROP TABLE IF EXISTS [active].[customer_acquistion_data]
GO

-- Create active table
CREATE TABLE [active].[customer_acquistion_data]
(
	[branch_id] int not null,
	[relationship_manager_id] int not null,
	[date] datetime not null,
	[customer_count] int null
) 
GO

--
-- Stored Proc. - Increment water mark
--

-- Drop procedure
DROP PROCEDURE IF EXISTS [stage].[increment_watermark]
GO

-- Create procedure
CREATE PROCEDURE [stage].[increment_watermark]
AS
BEGIN
	UPDATE [stage].[current_watermark]
	SET [process_date_hour] = DATEADD(dd, 1, [process_date_hour])
END
GO


--
-- View - formatted stage data
--

-- Drop view
DROP VIEW IF EXISTS [stage].[cleaned_customer_acquistion_data]
GO

-- Create view
CREATE VIEW [stage].[cleaned_customer_acquistion_data]
AS
SELECT 
	try_cast([branch_id] as int) as branch_id,
	try_cast([relationship_manager_id] as int) as relationship_manager_id,
	try_cast([date] as datetime) as date,
	try_cast([customer_count] as int) as customer_count
FROM 
	[stage].[customer_acquistion_data]
GO

--
-- Stored Proc. - upsert customer acquisition data
--

-- Drop procedure
DROP PROCEDURE IF EXISTS [stage].[upsert_customer_acquistion_data]
GO

-- Create procedure
CREATE PROCEDURE [stage].[upsert_customer_acquistion_data]
AS
BEGIN
	-- Set no count
	SET NOCOUNT ON 

	-- Merge the clean stage data with active table
	MERGE 
		[active].[customer_acquistion_data] AS trg
	USING 
	(
		SELECT * FROM [stage].[cleaned_customer_acquistion_data]
	) AS src 
	ON 
		src.[date] = trg.[date] and
		src.[relationship_manager_id] = trg.[relationship_manager_id] and
		src.[branch_id] = trg.[branch_id]

     -- Update condition
     WHEN MATCHED THEN 
         UPDATE SET
			[branch_id] = src.[branch_id],
			[relationship_manager_id] = src.[relationship_manager_id],
			[date] = src.[date],
			[customer_count] = src.[customer_count]
 
     -- Insert condition
     WHEN NOT MATCHED BY TARGET THEN
         INSERT
         (
			[branch_id],
			[relationship_manager_id],
			[date],
			[customer_count]
         )
         VALUES
         ( 
			src.[branch_id],
			src.[relationship_manager_id],
			src.[date],
			src.[customer_count]
         );
END
GO

/*    
    Show database objects
*/

SELECT *
FROM sys.objects
WHERE is_ms_shipped = 0
ORDER BY [name];