USE wtndwairline
GO
CREATE PROCEDURE sp_refreshmodel
AS

EXEC('
IF object_id(''airlines'') IS NOT NULL
	DROP TABLE airlines;

CREATE TABLE airlines WITH
	 (
		DISTRIBUTION = REPLICATE,
	    CLUSTERED COLUMNSTORE INDEX
	)
	AS
		SELECT  * FROM airlineinformation_dl;
	')

EXEC('
IF object_id(''airports'') IS NOT NULL
	DROP TABLE airports

CREATE TABLE airports WITH
	 (
		DISTRIBUTION = REPLICATE,
	    CLUSTERED COLUMNSTORE INDEX
	)
AS
	SELECT * FROM airportinformation_dl
	')

EXEC('
IF object_id(''performance'') IS NOT NULL
	DROP TABLE performance

CREATE TABLE performance WITH
	 (
		DISTRIBUTION = ROUND_ROBIN,
	    CLUSTERED COLUMNSTORE INDEX
	)
AS
	SELECT * FROM performance_dl
	')

EXEC('
IF object_id(''delayed'') IS NOT NULL
	DROP TABLE delayed

CREATE TABLE delayed WITH
	 (
		DISTRIBUTION = ROUND_ROBIN,
	    CLUSTERED COLUMNSTORE INDEX
	)
AS
	SELECT * FROM delayed_dl
	')

	
	EXEC('
IF object_id(''MonthlyDelayed'') IS NOT NULL
	DROP TABLE MonthlyDelayed

CREATE TABLE MonthlyDelayed WITH
	 (
		DISTRIBUTION = ROUND_ROBIN,
	    CLUSTERED COLUMNSTORE INDEX
	)
AS
	SELECT * FROM MonthlyDelayed_dl
	')
	
