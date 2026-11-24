[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "countryiso3code", DestinationColumnName = "countryiso3code"], [SourceColumnName = "year", DestinationColumnName = "year"], [SourceColumnName = "value", DestinationColumnName = "value"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Query = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json&per_page=1000")),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  Navigation = #"Converted to table"{1}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"countryiso3code", "date", "value"}, {"countryiso3code", "date", "value"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded Column1", {{"date", "year"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"countryiso3code", type text}, {"year", Int64.Type}, {"value", Int64.Type}}),
  RowCount = Table.RowCount(#"Changed column type"),

  Validated =
      if RowCount = 0 then
          error "World Bank API returned no data."
      else
          #"Changed column type"
in
  Validated;
shared Query_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[lakehouseName = "LH_Taxi_Analytics"]}[Data],
  Navigation_3 = Navigation_2{[Id = "bronze", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "world_bank_gdp", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
