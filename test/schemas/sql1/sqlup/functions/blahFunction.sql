CREATE FUNCTION [dbo].[blahFunction]()RETURNS [datetime] AS 
BEGIN
    RETURN DATEADD(millisecond, -2, CONVERT(datetime, '2004-07-01', 101));
END;
