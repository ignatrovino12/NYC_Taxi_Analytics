CREATE TABLE [dbo].[DimDate] (
    [date_key]    INT           NULL,
    [date]        DATE          NULL,
    [year]        INT           NULL,
    [quarter]     INT           NULL,
    [month]       INT           NULL,
    [month_name]  VARCHAR (MAX) NULL,
    [day]         INT           NULL,
    [day_of_week] INT           NULL,
    [day_name]    VARCHAR (MAX) NULL,
    [is_weekend]  BIT           NULL
);


GO