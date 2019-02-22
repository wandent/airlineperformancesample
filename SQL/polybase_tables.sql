

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '23987hxJ#KL95234nl0zBe';  
GO  

CREATE DATABASE SCOPED CREDENTIAL wtndatalakegen2_credential WITH IDENTITY = 'wtndatalake', Secret='Zq55yROlbXOonuLsxACg+61Dik5nx9Sl3ZVFeS50LXfeDC6FceT1fLdQIDUEzwYXxSz9p4HlhTe4NU8q/Pav8g==';
GO



CREATE EXTERNAL DATA SOURCE [airlines]
 WITH
  (
    TYPE = HADOOP, 
    LOCATION = 'abfss://airlines@wtndatalakegen2.dfs.core.windows.net/',
    CREDENTIAL = wtndatalakegen2_credential
);
GO


CREATE EXTERNAL FILE FORMAT processedfile  
WITH (FORMAT_TYPE = PARQUET );  
GO

if object_id('airlineinformation_dl') is not null
	DROP EXTERNAL TABLE airlineinformation_dl

CREATE EXTERNAL TABLE airlineinformation_dl 
	(
		code int, 
		description varchar(300)
	)   
    WITH (   
        LOCATION = '/processed/airlines.parquet',  
        DATA_SOURCE = airlines,  
        FILE_FORMAT = processedfile        
    )  
if object_id('airportinformation_dl') is not null
	DROP EXTERNAL TABLE airportinformation_dl

CREATE EXTERNAL TABLE airportinformation_dl 
	(
		code varchar(10), 
		description varchar(300)
	)   
    WITH (   
        LOCATION = '/processed/airports.parquet',  
        DATA_SOURCE = airlines,  
        FILE_FORMAT = processedfile        
    )
IF object_id('performance_dl') is not null 
	DROP EXTERNAL TABLE performance_dl
CREATE EXTERNAL TABLE performance_dl 
	(
		[Year] int,
		[Quarter] int,
		[Month] int,
		[DayofMonth] int,
		[DayOfWeek] int,
		FlightDate datetime2,
		UniqueCarrier varchar(200),
		AirlineID int,
		Carrier varchar(200),
		TailNum varchar(200),
		FlightNum int,
		OriginAirportID int,
		OriginAirportSeqID int,
		OriginCityMarketID int,
		Origin varchar(200),
		OriginCityName varchar(200),
		OriginState varchar(200),
		OriginStateFips int,
		OriginStateName varchar(200),
		OriginWac int,
		DestAirportID int,
		DestAirportSeqID int,
		DestCityMarketID int,
		Dest varchar(200),
		DestCityName varchar(200),
		DestState varchar(200),
		DestStateFips int,
		DestStateName varchar(200),
		DestWac int,
		CRSDepTime int,
		DepTime int,
		DepDelay float(53),
		DepDelayMinutes float(53),
		DepDel15 float(53),
		DepartureDelayGroups int,
		DepTimeBlk varchar(200),
		TaxiOut float(53),
		WheelsOff varchar(200),
		WheelsOn int,
		TaxiIn float(53),
		CRSArrTime int,
		ArrTime int,
		ArrDelay float(53),
		ArrDelayMinutes float(53),
		ArrDel15 float(53),
		ArrivalDelayGroups int,
		ArrTimeBlk varchar(200),
		Cancelled float(53),
		CancellationCode varchar(200),
		Diverted float(53),
		CRSElapsedTime float(53),
		ActualElapsedTime float(53),
		AirTime float(53),
		Flights float(53),
		Distance float(53),
		DistanceGroup int,
		CarrierDelay float(53),
		WeatherDelay float(53),
		NASDelay float(53),
		SecurityDelay float(53),
		LateAircraftDelay float(53),
		FirstDepTime int,
		TotalAddGTime float(53),
		LongestAddGTime float(53),
		DivAirportLandings int,
		DivReachedDest float(53),
		DivActualElapsedTime float(53),
		DivArrDelay float(53),
		DivDistance float(53),
		Div1Airport varchar(200),
		Div1AirportID int,
		Div1AirportSeqID int,
		Div1WheelsOn int,
		Div1TotalGTime float(53),
		Div1LongestGTime float(53),
		Div1WheelsOff int,
		Div1TailNum varchar(200),
		Div2Airport varchar(200),
		Div2AirportID int,
		Div2AirportSeqID int,
		Div2WheelsOn int,
		Div2TotalGTime float(53),
		Div2LongestGTime float(53),
		Div2WheelsOff int,
		Div2TailNum varchar(200),
		Div3Airport varchar(200),
		Div3AirportID int,
		Div3AirportSeqID int,
		Div3WheelsOn int,
		Div3TotalGTime float(53),
		Div3LongestGTime float(53),
		Div3WheelsOff varchar(200),
		Div3TailNum varchar(200),
		Div4Airport varchar(200),
		Div4AirportID varchar(200),
		Div4AirportSeqID varchar(200),
		Div4WheelsOn varchar(200),
		Div4TotalGTime varchar(200),
		Div4LongestGTime varchar(200),
		Div4WheelsOff varchar(200),
		Div4TailNum varchar(200),
		Div5Airport varchar(200),
		Div5AirportID varchar(200),
		Div5AirportSeqID varchar(200),
		Div5WheelsOn varchar(200),
		Div5TotalGTime varchar(200),
		Div5LongestGTime varchar(200),
		Div5WheelsOff varchar(200),
		Div5TailNum varchar(200),
		[_c109] varchar(200)

	)   
    WITH (   
        LOCATION = '/processed/performance-all.parquet',  
        DATA_SOURCE = airlines,  
        FILE_FORMAT = processedfile        
    )

	IF OBJECT_ID('delayed_dl') IS NOT NULL
		DROP EXTERNAL TABLE delayed_dl

	CREATE EXTERNAL TABLE delayed_dl 
	(
		[Quarter] integer,
		[Month] integer,
		[DayofMonth] integer,
		[DayOfWeek] integer,
		FlightDate datetime2,
		UniqueCarrier varchar(200),
		AirlineID integer,
		Carrier varchar(200),
		TailNum varchar(200),
		FlightNum integer,
		OriginAirportID integer,
		OriginAirportSeqID integer,
		OriginCityMarketID integer,
		Origin varchar(200),
		OriginCityName varchar(200),
		OriginState varchar(200),
		OriginStateFips integer,
		OriginStateName varchar(200),
		OriginWac integer,
		DestAirportID integer,
		DestAirportSeqID integer,
		DestCityMarketID integer,
		Dest varchar(200),
		DestCityName varchar(200),
		DestState varchar(200),
		DestStateFips integer,
		DestStateName varchar(200),
		DestWac integer,
		CRSDepTime integer,
		DepTime integer,
		DepDelay float(53),
		DepDelayMinutes float(53),
		DepDel15 float(53),
		DepartureDelayGroups integer,
		DepTimeBlk varchar(200),
		TaxiOut float(53),
		WheelsOff varchar(200),
		WheelsOn integer,
		TaxiIn float(53),
		CRSArrTime integer,
		ArrTime integer,
		ArrDelay float(53),
		ArrDelayMinutes float(53),
		ArrDel15 float(53),
		ArrivalDelayGroups integer,
		ArrTimeBlk varchar(200),
		Cancelled float(53),
		CancellationCode varchar(200),
		Diverted float(53),
		CRSElapsedTime float(53),
		ActualElapsedTime float(53),
		AirTime float(53),
		Flights float(53),
		Distance float(53),
		DistanceGroup integer,
		CarrierDelay float(53),
		WeatherDelay float(53),
		NASDelay float(53),
		SecurityDelay float(53),
		LateAircraftDelay float(53),
		FirstDepTime integer,
		TotalAddGTime float(53),
		LongestAddGTime float(53),
		DivAirportLandings integer,
		DivReachedDest float(53),
		DivActualElapsedTime float(53),
		DivArrDelay float(53),
		DivDistance float(53),
		Div1Airport varchar(200),
		Div1AirportID integer,
		Div1AirportSeqID integer,
		Div1WheelsOn integer,
		Div1TotalGTime float(53),
		Div1LongestGTime float(53),
		Div1WheelsOff integer,
		Div1TailNum varchar(200),
		Div2Airport varchar(200),
		Div2AirportID integer,
		Div2AirportSeqID integer,
		Div2WheelsOn integer,
		Div2TotalGTime float(53),
		Div2LongestGTime float(53),
		Div2WheelsOff integer,
		Div2TailNum varchar(200),
		Div3Airport varchar(200),
		Div3AirportID integer,
		Div3AirportSeqID integer,
		Div3WheelsOn integer,
		Div3TotalGTime float(53),
		Div3LongestGTime float(53),
		Div3WheelsOff varchar(200),
		Div3TailNum varchar(200),
		Div4Airport varchar(200),
		Div4AirportID varchar(200),
		Div4AirportSeqID varchar(200),
		Div4WheelsOn varchar(200),
		Div4TotalGTime varchar(200),
		Div4LongestGTime varchar(200),
		Div4WheelsOff varchar(200),
		Div4TailNum varchar(200),
		Div5Airport varchar(200),
		Div5AirportID varchar(200),
		Div5AirportSeqID varchar(200),
		Div5WheelsOn varchar(200),
		Div5TotalGTime varchar(200),
		Div5LongestGTime varchar(200),
		Div5WheelsOff varchar(200),
		Div5TailNum varchar(200),		
		_c109 varchar(200),
		Year int
	)   
    WITH (   
        LOCATION = '/processed/delayed-all.parquet',  
        DATA_SOURCE = airlines,  
        FILE_FORMAT = processedfile        
    )


	IF OBJECT_ID('MonthlyDelayed_dl') IS NOT NULL
		DROP EXTERNAL TABLE MonthlyDelayed_dl

	CREATE EXTERNAL TABLE MonthlyDelayed_dl 
	(
		[year] integer,
		[month] integer,
		UniqueCarrier varchar(200),
		TotalDelayed bigint,
		AverageDepartureDelay float(53)
	)   
    WITH (   
        LOCATION = '/processed/monthlydelayedflights-all.parquet',  
        DATA_SOURCE = airlines,  
        FILE_FORMAT = processedfile        
    )


--	select top 100 * from MonthlyDelayed_dl