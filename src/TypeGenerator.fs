module ORM.TypeGenerator

open System
open System.IO
open ORM.Schema
open ORM.SqlParser

module CodeGenerator =
    let toPascalCase (name: string) =
        name.Split('_')
        |> Array.map (fun part -> 
            if String.IsNullOrEmpty(part) then ""
            else part.[0].ToString().ToUpper() + part.Substring(1).ToLower())
        |> String.concat ""

    let toCamelCase (name: string) =
        let parts = name.Split('_', StringSplitOptions.RemoveEmptyEntries)
        match parts with
        | [||] -> name
        | [|first|] -> first.ToLower()
        | _ ->
            let first = parts.[0].ToLower()
            let rest = parts.[1..] |> Array.map (fun s -> 
                s.[0].ToString().ToUpper() + s.Substring(1).ToLower())
            first + String.concat "" rest

    let pgTypeToFSharpType (column: ColumnInfo) =
        let baseType =
            match column.DataType with
            | Int -> "int"
            | BigInt -> "int64"
            | Varchar _ | Text | Json -> "string"
            | Boolean -> "bool"
            | Date -> "System.DateTime"
        
        match baseType with
        | "string" -> baseType
        | _ when column.IsNullable -> baseType + " option"
        | _ -> baseType

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
        
        let lines = [
            sprintf "type %s = {" typeName
            fields
            "}"
        ]
        String.concat "\n" lines

    let generateAllTypes (tables: TableInfo list) =
        let headerLines = [
            "// AUTO-GENERATED FILE - DO NOT EDIT"
            "// Generated at: " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
            "// Generated from database schema"
            ""
            "namespace ORM.GeneratedTypes"
            ""
            "open System"
            ""
            "[<AttributeUsage(AttributeTargets.Property)>]"
            "type PrimaryKeyAttribute() ="
            "    inherit Attribute()"
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
            let tables = Parser.parseDatabaseSchema()
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
            ("PrimaryKeyAttribute", "Missing PrimaryKey attribute definition")
        ]
        
        let errors =
            validations
            |> List.filter (fun (text, _) -> not (generatedCode.Contains(text)))
            |> List.map snd
        
        if errors.IsEmpty then Ok () else Error errors