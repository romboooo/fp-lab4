// src/SqlParser.fs
module ORM.SqlParser

open System
open Npgsql
open ORM.Database
open ORM.Schema

module SchemaReader =
    
    let private mapDataType (dataType: string) (characterMaxLength: int option) =
        match dataType.ToLower() with
        | "integer" | "int" | "int4" -> Int
        | "bigint" | "int8" -> BigInt
        | "character varying" | "varchar" -> Varchar characterMaxLength
        | "text" -> Text
        | "boolean" | "bool" -> Boolean
        | "json" | "jsonb" -> Json
        | "date" -> Date
        | "timestamp" | "timestamp without time zone" -> Date
        | "numeric" | "decimal" -> Int
        | _ -> 
            printfn "Warning: Unknown type %s, mapping to Text" dataType
            Text
    
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
            let isNullable = reader.GetString(3) = "YES"
            
            let columnInfo = {
                Name = columnName
                DataType = mapDataType dataType charMaxLength
                IsNullable = isNullable
                IsPrimaryKey = false  // Будет установлено позже
                MaxLength = charMaxLength
            }
            
            columns.Add(columnInfo)
        
        reader.Close()
        columns |> Seq.toList
    
    let getPrimaryKeys (connection: NpgsqlConnection) (schema: string) (tableName: string) =
        let query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = @schema
                AND tc.table_name = @tableName
            ORDER BY kcu.ordinal_position
        """
        
        use cmd = new NpgsqlCommand(query, connection)
        cmd.Parameters.AddWithValue("@schema", schema) |> ignore
        cmd.Parameters.AddWithValue("@tableName", tableName) |> ignore
        
        use reader = cmd.ExecuteReader()
        let primaryKeys = ResizeArray<string>()
        
        while reader.Read() do
            primaryKeys.Add(reader.GetString(0))
        
        reader.Close()
        primaryKeys |> Seq.toList
    
    let getTableInfo (connection: NpgsqlConnection) (schema: string) (tableName: string) : TableInfo =
        let columns = getColumns connection schema tableName
        let primaryKeys = getPrimaryKeys connection schema tableName |> Set.ofSeq
        
        // Обновляем колонки, отмечая первичные ключи
        let columnsWithPK =
            columns
            |> List.map (fun col ->
                { col with IsPrimaryKey = Set.contains col.Name primaryKeys })
        
        {
            Schema = schema
            Name = tableName
            Columns = columnsWithPK
        }
    
    let getAllTablesInfo (dbConnection: DatabaseConnection) : TableInfo list =
        use conn = dbConnection.GetOpenConnection()
        let tables = getTables conn
        
        tables
        |> List.map (fun (schema, tableName) -> 
            getTableInfo conn schema tableName)

module Parser =
    let parseDatabaseSchema() : TableInfo list =
        let db = DatabaseConnection()
        SchemaReader.getAllTablesInfo db
    
    let formatColumnType (col: ColumnInfo) : string =
        match col.DataType with
        | Int -> "INT"
        | BigInt -> "BIGINT"
        | Varchar maxLen -> 
            match maxLen with
            | Some len -> sprintf "VARCHAR(%d)" len
            | None -> "VARCHAR"
        | Text -> "TEXT"
        | Boolean -> "BOOLEAN"
        | Json -> "JSON"
        | Date -> "DATE"
    
    let printDatabaseSchema() =
        let tables = parseDatabaseSchema()
        
        for table in tables do
            printfn "=== Table: %s.%s ===" table.Schema table.Name
            printfn "Columns:"
            printfn "--------------------------------------------------"
            printfn "%-20s %-20s %-10s %s" "Name" "Type" "Nullable" "PK"
            printfn "--------------------------------------------------"
            
            for col in table.Columns do
                let nullable = if col.IsNullable then "YES" else "NO"
                let pk = if col.IsPrimaryKey then "PK" else ""
                printfn "%-20s %-20s %-10s %s" 
                    col.Name 
                    (formatColumnType col)
                    nullable
                    pk
            
            printfn ""
    
    let generateSchemaSummary() =
        let tables = parseDatabaseSchema()
        
        let totalColumns = tables |> List.sumBy (fun t -> t.Columns.Length)
        
        printfn "Tables: %d" tables.Length
        printfn "Total columns: %d" totalColumns
        printfn ""
        
        for table in tables do
            let pkColumns = 
                table.Columns 
                |> List.filter (fun c -> c.IsPrimaryKey) 
                |> List.map (fun c -> c.Name)
            let pk = if pkColumns.IsEmpty then "No PK" 
                     else sprintf "PK: %s" (String.Join(", ", pkColumns))
            printfn "%-20s: %d columns, %s" table.Name table.Columns.Length pk