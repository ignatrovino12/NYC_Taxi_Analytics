CREATE TABLE [dbo].[FactAirQualityDaily] (
    [zone_key]           INT           NOT NULL,
    [parameter_name]     VARCHAR (MAX) NULL,
    [unit]               VARCHAR (MAX) NULL,
    [average_value]      FLOAT (53)    NULL,
    [total_measurements] BIGINT        NULL,
    [sensor_count]       BIGINT        NOT NULL,
    [date_key]           INT           NULL
);


GO