CREATE TABLE [dbo].[FactTaxiDaily] (
    [zone_key]                INT        NULL,
    [total_trips]             BIGINT     NOT NULL,
    [total_fares]             FLOAT (53) NULL,
    [average_fare]            FLOAT (53) NULL,
    [average_trip_distance]   FLOAT (53) NULL,
    [average_trip_duration]   FLOAT (53) NULL,
    [average_speed_mph]       FLOAT (53) NULL,
    [average_passenger_count] FLOAT (53) NULL,
    [date_key]                INT        NULL
);


GO