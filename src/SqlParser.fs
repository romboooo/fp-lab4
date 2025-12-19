module ORM.SqlParser

open System
open Npgsql
open ORM.Database

type ColumnType =
    | Int
    | BigInt
    | SmallInt
    | Varchar of maxLength: int option
    | Text
    | Decimal of precision: int * scale: int
    | Boolean
    | Timestamp
    | Date
    | Uuid
    | Json
    | Jsonb
    | Bytea
    | Serial
    | Custom of string

type ConstraintType =
    | PrimaryKey
    | ForeignKey of referencedTable: string * referencedColumn: string
    | Unique
    | Check
    | Default

type ColumnInfo = {
    Name: string
    DataType: ColumnType
    IsNullable: bool
    HasDefault: bool
    DefaultValue: string option
    MaxLength: int option
    NumericPrecision: int option
    NumericScale: int option
}

type ConstraintInfo = {
    Name: string
    Type: ConstraintType
    ColumnName: string
}

type TableInfo = {
    Schema: string
    Name: string
    Columns: ColumnInfo list
    Constraints: ConstraintInfo list
    PrimaryKeyColumns: string list
}

module SchemaReader =
    
    let private mapDataType (dataType: string) (characterMaxLength: int option) (numericPrecision: int option) (numericScale: int option) =
        match dataType.ToLower() with
        | "integer" | "int" | "int4" -> 
            if dataType.Contains("serial") || dataType.Contains("nextval") then Serial
            else Int
        | "bigint" | "int8" -> BigInt
        | "smallint" | "int2" -> SmallInt
        | "character varying" | "varchar" -> Varchar characterMaxLength
        | "text" -> Text
        | "numeric" | "decimal" -> 
            match numericPrecision, numericScale with
            | Some p, Some s -> Decimal(p, s)
            | Some p, None -> Decimal(p, 0)
            | None, Some s -> Decimal(10, s)
            | None, None -> Decimal(10, 2)
        | "boolean" | "bool" -> Boolean
        | "timestamp without time zone" | "timestamp" -> Timestamp
        | "timestamp with time zone" | "timestamptz" -> Timestamp
        | "date" -> Date
        | "uuid" -> Uuid
        | "json" -> Json
        | "jsonb" -> Jsonb
        | "bytea" -> Bytea
        | custom -> Custom custom
    
    let getTables (connection: NpgsqlConnection) =
        let query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        
        use cmd = new NpgsqlCommand(query, connection)
        use reader = cmd.ExecuteReader()
        
        let tables = ResizeArray<string * string>()
        while reader.Read() do
            tables.Add(reader.GetString(0), reader.GetString(1))
        
        reader.Close()
        tables |> Seq.toList
    
    let getColumns (connection: NpgsqlConnection) (schema: string) (tableName: string) =
        let query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = @schema
              AND table_name = @tableName
            ORDER BY ordinal_position
        """
        
        use cmd = new NpgsqlCommand(query, connection)
        cmd.Parameters.AddWithValue("@schema", schema) |> ignore
        cmd.Parameters.AddWithValue("@tableName", tableName) |> ignore
        
        use reader = cmd.ExecuteReader()
        let columns = ResizeArray<ColumnInfo>()
        
        while reader.Read() do
            let columnName = reader.GetString(0)
            let dataType = reader.GetString(1)
            let charMaxLength = if reader.IsDBNull(2) then None else Some (reader.GetInt32(2))
            let numericPrecision = if reader.IsDBNull(3) then None else Some (reader.GetInt32(3))
            let numericScale = if reader.IsDBNull(4) then None else Some (reader.GetInt32(4))
            let isNullable = reader.GetString(5) = "YES"
            let hasDefault = not (reader.IsDBNull(6))
            let defaultValue = if hasDefault then Some (reader.GetString(6)) else None
            
            let columnInfo = {
                Name = columnName
                DataType = mapDataType dataType charMaxLength numericPrecision numericScale
                IsNullable = isNullable
                HasDefault = hasDefault
                DefaultValue = defaultValue
                MaxLength = charMaxLength
                NumericPrecision = numericPrecision
                NumericScale = numericScale
            }
            
            columns.Add(columnInfo)
        
        reader.Close()
        columns |> Seq.toList
    
    let getConstraints (connection: NpgsqlConnection) (schema: string) (tableName: string) =
        let query = """
            SELECT
                tc.constraint_name,
                tc.constraint_type,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            LEFT JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            WHERE tc.table_schema = @schema
              AND tc.table_name = @tableName
              AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK')
            ORDER BY tc.constraint_name, kcu.ordinal_position
        """
        
        use cmd = new NpgsqlCommand(query, connection)
        cmd.Parameters.AddWithValue("@schema", schema) |> ignore
        cmd.Parameters.AddWithValue("@tableName", tableName) |> ignore
        
        use reader = cmd.ExecuteReader()
        let constraints = ResizeArray<ConstraintInfo>()
        let primaryKeys = ResizeArray<string>()
        
        while reader.Read() do
            let constraintName = reader.GetString(0)
            let constraintType = reader.GetString(1)
            let columnName = if reader.IsDBNull(2) then "" else reader.GetString(2)
            
            let constraintTypeParsed =
                match constraintType with
                | "PRIMARY KEY" -> 
                    primaryKeys.Add(columnName)
                    PrimaryKey
                | "FOREIGN KEY" ->
                    let foreignTable = reader.GetString(3)
                    let foreignColumn = reader.GetString(4)
                    ForeignKey(foreignTable, foreignColumn)
                | "UNIQUE" -> Unique
                | "CHECK" -> Check
                | _ -> failwithf "Unknown constraint type: %s" constraintType
            
            let constraintInfo = {
                Name = constraintName
                Type = constraintTypeParsed
                ColumnName = columnName
            }
            
            constraints.Add(constraintInfo)
        
        reader.Close()
        (constraints |> Seq.toList, primaryKeys |> Seq.toList)
    
    let getTableInfo (connection: NpgsqlConnection) (schema: string) (tableName: string) =
        let columns = getColumns connection schema tableName
        let (constraints, primaryKeys) = getConstraints connection schema tableName
        
        {
            Schema = schema
            Name = tableName
            Columns = columns
            Constraints = constraints
            PrimaryKeyColumns = primaryKeys
        }
    
    let getAllTablesInfo (dbConnection: DatabaseConnection) =
        use conn = dbConnection.GetOpenConnection()
        let tables = getTables conn
        
        tables
        |> List.map (fun (schema, tableName) -> 
            getTableInfo conn schema tableName)
    
    let formatColumnType (colType: ColumnType) =
        match colType with
        | Int -> "INT"
        | BigInt -> "BIGINT"
        | SmallInt -> "SMALLINT"
        | Varchar maxLen -> 
            match maxLen with
            | Some len -> sprintf "VARCHAR(%d)" len
            | None -> "VARCHAR"
        | Text -> "TEXT"
        | Decimal(p, s) -> sprintf "DECIMAL(%d,%d)" p s
        | Boolean -> "BOOLEAN"
        | Timestamp -> "TIMESTAMP"
        | Date -> "DATE"
        | Uuid -> "UUID"
        | Json -> "JSON"
        | Jsonb -> "JSONB"
        | Bytea -> "BYTEA"
        | Serial -> "SERIAL"
        | Custom custom -> custom.ToUpper()
    
    let formatConstraint (constr: ConstraintInfo) =
        match constr.Type with
        | PrimaryKey -> sprintf "%s: PRIMARY KEY(%s)" constr.Name constr.ColumnName
        | ForeignKey(refTable, refColumn) -> 
            sprintf "%s: FOREIGN KEY(%s) REFERENCES %s(%s)" constr.Name constr.ColumnName refTable refColumn
        | Unique -> sprintf "%s: UNIQUE(%s)" constr.Name constr.ColumnName
        | Check -> sprintf "%s: CHECK" constr.Name
        | _ -> constr.Name
    
    let formatTableInfo (table: TableInfo) =
        let sb = System.Text.StringBuilder()
        
        sb.AppendLine(sprintf "=== Table: %s.%s ===" table.Schema table.Name) |> ignore
        
        sb.AppendLine("\nColumns:") |> ignore
        sb.AppendLine("--------------------------------------------------") |> ignore
        sb.AppendLine(sprintf "%-20s %-20s %-10s %-10s" "Name" "Type" "Nullable" "Default") |> ignore
        sb.AppendLine("--------------------------------------------------") |> ignore
        
        for col in table.Columns do
            let nullable = if col.IsNullable then "YES" else "NO"
            let defaultValue = 
                match col.DefaultValue with
                | Some v when v.Contains("nextval") -> "AUTO_INCREMENT"
                | Some v -> 
                    let truncated = if v.Length > 20 then v.Substring(0, 20) + "..." else v
                    truncated
                | None -> ""
            
            sb.AppendLine(sprintf "%-20s %-20s %-10s %-10s" 
                col.Name (formatColumnType col.DataType) nullable defaultValue) |> ignore
        
        if not table.PrimaryKeyColumns.IsEmpty then
            sb.AppendLine(sprintf "\nPrimary Key: %s" (String.Join(", ", table.PrimaryKeyColumns))) |> ignore
        
        if not table.Constraints.IsEmpty then
            sb.AppendLine("\nConstraints:") |> ignore
            for constr in table.Constraints do
                sb.AppendLine(sprintf "  %s" (formatConstraint constr)) |> ignore
        
        sb.ToString()

module Parser =
    let parseDatabaseSchema() =
        let db = DatabaseConnection()
        SchemaReader.getAllTablesInfo db
    
    let printDatabaseSchema() =
        let tables = parseDatabaseSchema()
        
        printfn "\n=== Database Schema Analysis ==="
        printfn "Total tables found: %d\n" tables.Length
        
        for table in tables do
            printfn "%s" (SchemaReader.formatTableInfo table)
            printfn ""
    
    let generateSchemaSummary() =
        let tables = parseDatabaseSchema()
        
        let totalColumns = tables |> List.sumBy (fun t -> t.Columns.Length)
        let totalConstraints = tables |> List.sumBy (fun t -> t.Constraints.Length)
        
        printfn "=== Database Schema Summary ==="
        printfn "Tables: %d" tables.Length
        printfn "Total columns: %d" totalColumns
        printfn "Total constraints: %d" totalConstraints
        printfn ""
        
        for table in tables do
            let pk = if table.PrimaryKeyColumns.IsEmpty then "No PK" 
                     else sprintf "PK: %s" (String.Join(", ", table.PrimaryKeyColumns))
            printfn "%-20s: %d columns, %s" table.Name table.Columns.Length pk