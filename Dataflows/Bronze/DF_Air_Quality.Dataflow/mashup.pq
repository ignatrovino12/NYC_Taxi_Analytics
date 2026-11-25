[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared OPENAQ_API_KEY = "9d39e186b53700fde47d9277cc5de9e6f48397cef1a1001b318e79b9f4d73b4e" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared DateFrom = #date(2016, 1, 1) meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type date, DefaultValue = null];
shared DateTo = #date(2026, 10, 30) meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type date, DefaultValue = null];
shared ChunkStartDate = #date(2010, 1, 1) meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type date, DefaultValue = null];
shared ChunkEndDate = #date(2016, 9, 30) meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type date, DefaultValue = null];
shared Query_Locations = let
    Response = Json.Document(
        Web.Contents(
            "https://api.openaq.org",
            [
                RelativePath = "v3/locations",
                Query = [
                    coordinates = "40.7128,-74.0060",
                    radius = "25000",
                    limit = "1000",
                    page = "1"
                ],
                Headers = [
                    #"X-API-Key" = OPENAQ_API_KEY
                ]
            ]
        )
    ),

    Results = Response[results],
    Output = Table.FromRecords(Results),
  #"Removed other columns" = Table.SelectColumns(Output, {"name", "locality", "id", "coordinates", "datetimeFirst", "datetimeLast", "sensors"})
in
    #"Removed other columns";
shared Query_Sensors_Last_Dates = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navigation 1" = Source{[workspaceId = "."]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseName = "LH_Taxi_Analytics"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "bronze.sensor_checkpoints", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared Query_Sensors = let
    Locations = Query_Locations,

  #"Removed columns" = Table.RemoveColumns(Locations, {"locality"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"id", "location_id"}}),
  #"Expanded coordinates" = Table.ExpandRecordColumn(#"Renamed columns", "coordinates", {"latitude", "longitude"}, {"latitude", "longitude"}),
  #"Expanded datetimeFirst" = Table.ExpandRecordColumn(#"Expanded coordinates", "datetimeFirst", {"utc"}, {"utc"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Expanded datetimeFirst", {{"utc", "datetimeFirst_utc"}}),
  #"Expanded datetimeLast" = Table.ExpandRecordColumn(#"Renamed columns 1", "datetimeLast", {"utc"}, {"datetimeLast.utc"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Expanded datetimeLast", {{"datetimeLast.utc", "datetimeLast_utc"}}),
  #"Expanded sensors" = Table.ExpandListColumn(#"Renamed columns 2", "sensors"),
  #"Expanded sensors 1" = Table.ExpandRecordColumn(#"Expanded sensors", "sensors", {"id", "parameter"}, {"id", "parameter"}),
  #"Expanded parameter" = Table.ExpandRecordColumn(#"Expanded sensors 1", "parameter", {"name", "units"}, {"name.1", "units"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Expanded parameter", {{"id", "sensor_id"}, {"name.1", "parameter_name"}}),
  #"Filtered rows" = Table.SelectRows(#"Renamed columns 3", each ([parameter_name] = "no2" or [parameter_name] = "pm25")),
  #"Merged queries" = Table.ExpandTableColumn(Table.NestedJoin(#"Filtered rows",{"sensor_id"},Query_Sensors_Last_Dates,{"sensor_id"},"Checkpoint",JoinKind.LeftOuter),
  "Checkpoint",{"last_date"},{"last_date"}),
  #"Added ingest_start_date" = Table.AddColumn(#"Merged queries","ingest_start_date",each if [last_date] = null then DateFrom else [last_date],type date)
in
  #"Added ingest_start_date";
shared Get_Sensor_Data = (sensor_id as number, ingest_start_date as date, ChunkStartDate as date, ChunkEndDate as date) =>
let
    GetPage = (Page as number, FromDate as date, ToDate as date) as record =>
        let
            Request = try              
                Web.Contents(
                    "https://api.openaq.org",
                    [
                        RelativePath = "v3/sensors/" & Text.From(sensor_id) & "/hours",
                        Query = [
                            datetime_from = Date.ToText(FromDate, "yyyy-MM-dd") & "T00:00:00",
                            datetime_to = Date.ToText(ToDate, "yyyy-MM-dd") & "T23:59:59",
                            limit = "1000",
                            page = Text.From(Page)],
                        Headers = [#"X-API-Key" = OPENAQ_API_KEY],
                        Timeout = #duration(0, 0, 2, 0),
                        ManualStatusHandling = {429}
                    ]
                ),
            Result =
                if Request[HasError] then
                    [ Success = false, Data = {}, Error = Request[Error], Status = null, RetryAfter = null]
                else
                    let
                        Response = Request[Value],
                        Metadata = Value.Metadata(Response),
                        Status = try Metadata[Response.Status] otherwise 200,
                        Headers = try Metadata[Headers] otherwise [],
                        RetryHeader =
                            try Headers[#"x-ratelimit-reset"]
                            otherwise try Headers[#"X-RateLimit-Reset"]
                            otherwise null,
                        RetrySeconds =
                            try Number.FromText(Text.From(RetryHeader))
                            otherwise null,
                        Parsed = try Json.Document(Response)
                    in
                        if Status = 429 then
                            [Success = false,  Data = {}, Error = "HTTP 429 - Too Many Requests", Status = 429, RetryAfter = RetrySeconds]
                        else if Parsed[HasError] then
                            [Success = false, Data = {}, Error = Parsed[Error], Status = Status, RetryAfter = null]
                        else
                            [Success = true, Data = Parsed[Value][results], Error = null,  Status = Status, RetryAfter = null]
        in
            Result,

    GetPageWithRetry = (Page as number, FromDate as date, ToDate as date) as record =>
        let
            FirstAttempt = GetPage(Page, FromDate, ToDate),

            WaitSeconds =
            if FirstAttempt[Status] = 429 then
                if FirstAttempt[RetryAfter] <> null and FirstAttempt[RetryAfter] > 0 then
                    FirstAttempt[RetryAfter] + 2
                else
                    60
            else
                5,

            SecondAttempt =
                if FirstAttempt[Success] then
                    FirstAttempt
                else
                    Function.InvokeAfter(
                        () => GetPage(Page, FromDate, ToDate),
                        #duration(0, 0, 0, WaitSeconds)
                    ),

            Result =
                if FirstAttempt[Success] then
                    FirstAttempt
                else if SecondAttempt[Success] then
                    SecondAttempt
                else
                    [
                        Success = false,
                        Data = {},
                        Error = [
                            FirstAttempt = FirstAttempt[Error],
                            SecondAttempt = SecondAttempt[Error]
                        ],
                        Status = SecondAttempt[Status],
                        RetryAfter = SecondAttempt[RetryAfter]
                    ]
        in
            Result,

                
    FromDate =
        if ingest_start_date > ChunkStartDate
        then ingest_start_date
        else ChunkStartDate,

    ToDate =
        if DateTo < ChunkEndDate
        then DateTo
        else ChunkEndDate,

    Chunk =
        if FromDate <= ToDate then
            [FromDate = FromDate, ToDate = ToDate]
        else
            null,

    GetChunk = (Chunk as record) =>
    let
        Pages = List.Generate(
            () => [ Page = 1, Result = GetPageWithRetry( 1, Chunk[FromDate], Chunk[ToDate]), Continue = true],

            each [Continue],

            each
                if not [Result][Success] then
                    [ Page = [Page], Result = [Result], Continue = false]
                else if List.Count([Result][Data]) = 1000 then
                    let
                        NextPage = [Page] + 1,
                        NextResult =
                            Function.InvokeAfter(
                                () => GetPageWithRetry( NextPage, Chunk[FromDate], Chunk[ToDate]), #duration(0, 0, 0, 5))
                    in
                        [ Page = NextPage, Result = NextResult, Continue = true]
                else
                    [ Page = [Page], Result = [Result], Continue = false],

            each
                if [Result][Success] then
                    [Result][Data]
                else
                    let
                        ErrorMessage =
                            let
                                FirstError =
                                    try [Result][Error][FirstAttempt][Message]
                                    otherwise try [Result][Error][FirstAttempt]
                                    otherwise "First error unavailable",

                                SecondError =
                                    try [Result][Error][SecondAttempt][Message]
                                    otherwise try [Result][Error][SecondAttempt]
                                    otherwise "Second error unavailable"
                            in
                                "First attempt: " & (try Text.From(FirstError) otherwise "Unknown") &
                                " | Second attempt: " & (try Text.From(SecondError) otherwise "Unknown"),

                        Diagnostic =
                            "OpenAQ request failed after retry. " &
                            "SensorId: " & Text.From(sensor_id) &
                            " | Page: " & Text.From([Page]) &
                            " | FromDate: " & Date.ToText(Chunk[FromDate], "yyyy-MM-dd") &
                            " | ToDate: " & Date.ToText(Chunk[ToDate], "yyyy-MM-dd") &
                            " | HTTP Status: " &
                                (if [Result][Status] = null then "null"
                                else Text.From([Result][Status])) &
                            " | RetryAfter: " &
                                (if [Result][RetryAfter] = null then "null"
                                else Text.From([Result][RetryAfter])) &
                            " | OriginalError: " & ErrorMessage
                    in
                        error Error.Record(
                            "OpenAQ Diagnostic",
                            Diagnostic,
                            [SensorId = sensor_id, Page = [Page]]
                        )
                    )
    in
        List.Combine(Pages),

    AllData =
        if Chunk = null then
            {}
        else
            GetChunk(Chunk),

    Output = Table.FromRecords(AllData)

in
    Output;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_Air_Quality_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "location_id", DestinationColumnName = "location_id"], [SourceColumnName = "latitude", DestinationColumnName = "latitude"], [SourceColumnName = "longitude", DestinationColumnName = "longitude"], [SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"], [SourceColumnName = "parameter_name", DestinationColumnName = "parameter_name"], [SourceColumnName = "units", DestinationColumnName = "units"], [SourceColumnName = "interval", DestinationColumnName = "interval"], [SourceColumnName = "datetime_from_utc", DestinationColumnName = "datetime_from_utc"], [SourceColumnName = "datetime_to_utc", DestinationColumnName = "datetime_to_utc"], [SourceColumnName = "value", DestinationColumnName = "value"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared Query_Air_Quality = let
    Source = Query_Sensors,

  #"Added Measurements" = Table.AddColumn(Source,"Measurements",
        each Get_Sensor_Data([sensor_id],if [last_date] = null then DateFrom else [last_date],ChunkStartDate,ChunkEndDate)),
  #"Filtered Measurements" = Table.SelectRows(#"Added Measurements",each not Table.IsEmpty([Measurements])),
  #"Expanded Measurements" = Table.ExpandTableColumn(#"Filtered Measurements", "Measurements", {"value", "period"}, {"value", "period"}),
  #"Expanded period" = Table.ExpandRecordColumn(#"Expanded Measurements", "period", {"interval", "datetimeFrom", "datetimeTo"}, {"interval", "datetimeFrom", "datetimeTo"}),
  #"Expanded datetimeFrom" = Table.ExpandRecordColumn(#"Expanded period", "datetimeFrom", {"utc"}, {"utc"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded datetimeFrom", {{"utc", "datetime_from_utc"}}),
  #"Expanded datetimeTo" = Table.ExpandRecordColumn(#"Renamed columns", "datetimeTo", {"utc"}, {"utc"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Expanded datetimeTo", {{"utc", "datetime_to_utc"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns 1", {"datetimeLast_utc", "datetimeFirst_utc", "last_date", "ingest_start_date"}),
  #"Changed Type" = Table.TransformColumnTypes(#"Removed columns",
    {
        {"name", type text},
        {"location_id", Int64.Type},
        {"latitude", type number},
        {"longitude", type number},
        {"sensor_id", Int64.Type},
        {"parameter_name", type text},
        {"units", type text},
        {"interval", type text},
        {"datetime_from_utc", type datetime},
        {"datetime_to_utc", type datetime},
        {"value", type number}
    }),
  #"Reordered columns" = Table.ReorderColumns(#"Changed Type", {"name", "location_id", "latitude", "longitude", "sensor_id", "parameter_name", "units", "interval", "datetime_from_utc", "datetime_to_utc", "value"})
in
    #"Reordered columns";
shared Query_Air_Quality_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[lakehouseName = "LH_Taxi_Analytics"]}[Data],
  Navigation_3 = Navigation_2{[Id = "bronze", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "air_quality_measurements", ItemKind = "Table"]}[Data]
in
  TableNavigation;
