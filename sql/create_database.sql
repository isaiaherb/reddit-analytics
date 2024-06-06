IF NOT EXISTS (
    SELECT name 
    FROM sys.databases 
    WHERE name = N'reddit'
)
BEGIN
    CREATE DATABASE reddit;
    PRINT 'Database created successfully.';
END ELSE
BEGIN
    PRINT 'Database already exists.';
END
