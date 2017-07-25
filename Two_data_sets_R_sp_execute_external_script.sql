/*
Author: Tomaz Kastrun
Date: 25.07.2017

How to pass two datasets (respectively queries) into sp_execute_external_script

*/

USE SQLR;
GO

DROP TABLE IF EXISTS dataset;
GO

CREATE TABLE dataset
(ID INT IDENTITY(1,1) NOT NULL
,v1 INT
,v2 INT
CONSTRAINT pk_dataset PRIMARY KEY (id)
)

SET NOCOUNT ON;
GO 

INSERT INTO dataset(v1,v2)
SELECT TOP 1
 (SELECT TOP 1 number FROM master..spt_values WHERE type IN ('EOB') ORDER BY NEWID()) AS V1
,(SELECT TOP 1 number FROM master..spt_values WHERE type IN ('EOD') ORDER BY NEWID()) AS v2
FROM master..spt_values
GO 50


EXEC sp_execute_external_script
	 @language = N'R'
	,@script = N'OutputDataSet <- data.frame(MySet);'
	,@input_data_1 = N'SELECT TOP 5 v1, v2 FROM dataset;'
	,@input_data_1_name = N'MySet'
WITH RESULT SETS
((
    Val1 INT
   ,Val2 INT
))


--- Adding an additional second / external source




CREATE TABLE external_dataset
(ID INT IDENTITY(1,1) NOT NULL
,v1 INT
CONSTRAINT pk_external_dataset PRIMARY KEY (id)
)

SET NOCOUNT ON;
GO 

INSERT INTO external_dataset(v1)
SELECT TOP 1
 (SELECT TOP 1 number FROM master..spt_values WHERE type IN ('EOB') ORDER BY NEWID()) AS V1
FROM master..spt_values
GO 50

SELECT TOP 5 id, v1 FROM external_dataset


/*

USE [master]
GO
CREATE LOGIN [RR] WITH PASSWORD=N'Read!2$16', DEFAULT_DATABASE=[SQLR], CHECK_EXPIRATION=ON, CHECK_POLICY=ON
GO
ALTER SERVER ROLE [sysadmin] ADD MEMBER [RR]
GO
USE [SQLR]
GO
CREATE USER [RR] FOR LOGIN [RR]
GO
USE [SQLR]
GO
ALTER USER [RR] WITH DEFAULT_SCHEMA=[dbo]
GO
USE [SQLR]
GO
ALTER ROLE [db_datareader] ADD MEMBER [RR]
GO
USE [SQLR]
GO
ALTER ROLE [db_datawriter] ADD MEMBER [RR]
GO
USE [SQLR]
GO
ALTER ROLE [db_owner] ADD MEMBER [RR]
GO



*/


EXECUTE AS USER = 'RR';  
GO

DECLARE @Rscript NVARCHAR(MAX)
-- SET @Rscript = 'OutputDataSet <- data.frame(MySet);'
SET @Rscript = '
			library(RODBC)
			myconn <-odbcDriverConnect("driver={SQL Server};Server=SICN-KASTRUN;database=SQLR;uid=RR;pwd=Read!2$16")
		
			External_source <- sqlQuery(myconn, "SELECT v1 AS v3 FROM external_dataset")
			close(myconn) 
			Myset <- data.frame(MySet)
			# Merge both datasets
			mergeDataSet <- data.frame(cbind(Myset, External_source));'

EXEC sp_execute_external_script
	 @language = N'R'
	,@script = @Rscript
	,@input_data_1 = N'SELECT v1, v2 FROM dataset;'
	,@input_data_1_name = N'MySet'
	,@output_data_1_name = N'mergeDataSet'
WITH RESULT SETS
((
    Val1 INT
   ,Val2 INT
   ,Val3 INT
))

-- Check the results!
SELECT * FROM dataset
SELECT * FROM external_dataset

REVERT;
GO
