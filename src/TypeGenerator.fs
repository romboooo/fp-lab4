module ORM.TypeGenerator

open System
open System.IO
open ORM.Schema
open ORM 

module CodeGenerator =
    let toPascalCase (name: string) =
        name.Split('_')
        |> Array.map (fun part -> 
            if String.IsNullOrEmpty(part) then ""
            else Char.ToUpper(part.[0]).ToString() + part.Substring(1).ToLower())
        |> String.concat ""

    let toCamelCase (name: string) =
        let parts = name.Split('_', StringSplitOptions.RemoveEmptyEntries)
        match parts with
        | [||] -> name
        | _ ->
            let first = parts.[0].ToLower()
            let rest = parts.[1..] |> Array.map (fun s -> 
                Char.ToUpper(s.[0]).ToString() + s.Substring(1).ToLower())
            first + String.concat "" rest

    let pgTypeToFSharpType (column: ColumnInfo) =
        let baseType =
            match column.DataType with
            | Int -> "int"
            | BigInt -> "int64"
            | Varchar _ | Text | Json -> "string"
            | Boolean -> "bool"
            | Date -> "System.DateTime"
            | Float -> if column.IsNullable then "float option" else "float"
        
        if column.IsNullable then
            baseType + " option"
        else
            baseType

    let generateRecordType (table: TableInfo) =
        let typeName = toPascalCase table.Name
        
        let fields =
            table.Columns
            |> List.map (fun col ->
                let fieldName = toCamelCase col.Name
                let fieldType = pgTypeToFSharpType col
                if col.IsPrimaryKey then
                    sprintf "    [<PrimaryKey>] %s: %s" fieldName fieldType
                else
                    sprintf "    %s: %s" fieldName fieldType)
            |> String.concat "\n"
        
        sprintf "type %s = {\n%s\n}" typeName fields

    let generateAllTypes (tables: TableInfo list) =
        let headerLines = [
            "namespace ORM.GeneratedTypes"
            ""
            "// AUTO-GENERATED FILE - DO NOT EDIT"
            sprintf "// Generated at: %s" (DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"))
            "// Generated from database schema"
            ""
            "open System"
            "open ORM"  
            ""
        ]
        
        let typeLines = 
            tables
            |> List.map generateRecordType
            |> List.collect (fun s -> [s; ""])
        
        String.concat "\n" (headerLines @ typeLines)

module Generator =
    let generateTypes () =
        try
            let tables = ORM.SqlParser.Parser.parseDatabaseSchema()
            let generatedCode = CodeGenerator.generateAllTypes tables
            
            let generatedDir = "generated"
            let outputPath = Path.Combine(generatedDir, "GeneratedTypes.fs")
            
            if not (Directory.Exists(generatedDir)) then
                Directory.CreateDirectory(generatedDir) |> ignore
            
            File.WriteAllText(outputPath, generatedCode, System.Text.Encoding.UTF8)
            
            Ok generatedCode
            
        with ex ->
            Error ex.Message

    let validateGeneratedTypes (generatedCode: string) =
        let validations = [
            ("namespace ORM.GeneratedTypes", "Missing namespace")
            ("type ", "No types generated")
            ("[<PrimaryKey>]", "PrimaryKey attribute not found in generated code")
        ]
        
        let errors =
            validations
            |> List.filter (fun (text, _) -> not (generatedCode.Contains(text)))
            |> List.map snd
        
        if errors.IsEmpty then Ok () else Error errors