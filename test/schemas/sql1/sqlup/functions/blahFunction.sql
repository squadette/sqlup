
CREATE FUNCTION [dbo].[blahFunction](@Status [tinyint])
RETURNS [nvarchar](15) 
AS 
-- Returns the sales order status text representation for the status value.
BEGIN
    DECLARE @ret [nvarchar](15);

    SET @ret = 
        CASE @Status
            WHEN 1 THEN 'Pending approval'
            WHEN 2 THEN 'Approved'
            WHEN 3 THEN 'Obsolete'
            ELSE '** Invalid **'
        END;
    
    RETURN @ret
END;
