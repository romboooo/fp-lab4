module ORM.TypeGenerator

open System
open System.Text
open ORM.Schema
open ORM.SqlParser

module CodeGenerator =
    let toPascalCase (name: string) : string =
        name.Split('_')
        |> Array.map (fun part -> 
            if String.IsNullOrEmpty(part) then ""
            else part.[0].ToString().ToUpper() + part.Substring(1).ToLower())
        |> String.concat ""

    let toCamelCase (name: string) : string =
        let parts = name.Split('_', StringSplitOptions.RemoveEmptyEntries)
        match parts with
        | [||] -> name
        | [|first|] -> first.ToLower()
        | _ ->
            let first = parts.[0].ToLower()
            let rest = parts.[1..] |> Array.map (fun s -> 
                s.[0].ToString().ToUpper() + s.Substring(1).ToLower())
            first + String.concat "" rest

    let pgTypeToFSharpType (column: ColumnInfo) : string =
        let baseType =
            match column.DataType with
            | Int -> "int"
            | BigInt -> "int64"
            | Varchar _ | Text -> "string"
            | Boolean -> "bool"
            | Json -> "string"
            | Date -> "System.DateTime"
        
        match baseType with
        | "string" -> baseType
        | _ when column.IsNullable -> baseType + " option"
        | _ -> baseType

    let generateRecordType (table: TableInfo) : string =
        let typeName = toPascalCase table.Name
        
        let fields =
            table.Columns
            |> List.map (fun col ->
                let fieldName = toCamelCase col.Name
                let fieldType = pgTypeToFSharpType col
                let pkAttr = if col.IsPrimaryKey then "[<PrimaryKey>] " else ""
                sprintf "    %s%s: %s" pkAttr fieldName fieldType)
            |> String.concat "\n"
        
        sprintf """[<CLIMutable>]
        type %s = {
        %s
        }""" typeName fields

    let generatePrimaryKeyAttribute() : string =
        """[<AttributeUsage(AttributeTargets.Property)>]
type PrimaryKeyAttribute() =
    inherit Attribute()"""

    let generateAll (tables: TableInfo list) : string =
        let sb = StringBuilder()
        
        sb.AppendLine("// AUTO-GENERATED FILE") |> ignore
        sb.AppendLine("// Generated at: " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")) |> ignore
        sb.AppendLine("// DO NOT EDIT MANUALLY") |> ignore
        sb.AppendLine() |> ignore
        
        sb.AppendLine("namespace ORM.GeneratedTypes") |> ignore
        sb.AppendLine() |> ignore
        
        sb.AppendLine("open System") |> ignore
        sb.AppendLine() |> ignore
        
        sb.AppendLine(generatePrimaryKeyAttribute()) |> ignore
        sb.AppendLine() |> ignore
        
        // Генерация типов
        tables
        |> List.iter (fun table ->
            sb.AppendLine(generateRecordType table) |> ignore
            sb.AppendLine() |> ignore)
        
        sb.ToString()

module FileWriter =
    open System.IO
    
    let writeToFile (content: string) (outputPath: string) : Result<unit, string> =
        try
            let directory = Path.GetDirectoryName(outputPath)
            if not (String.IsNullOrEmpty(directory)) && not (Directory.Exists(directory)) then
                Directory.CreateDirectory(directory) |> ignore
            
            File.WriteAllText(outputPath, content, Encoding.UTF8)
            Ok ()
        with ex ->
            Error ex.Message

module Generator =
    let generateTypes (outputPath: string) : Result<string, string> =
        try
            let tables = Parser.parseDatabaseSchema()
            let generatedCode = CodeGenerator.generateAll tables
            
            match FileWriter.writeToFile generatedCode outputPath with
            | Ok _ -> 
                printfn "Types generated successfully at: %s" outputPath
                Ok generatedCode
            | Error err -> Error err
            
        with ex ->
            Error ex.Message
    
    let validateGeneratedTypes (generatedCode: string) : Result<unit, string list> =
        let validations = [
            ("namespace ORM.GeneratedTypes", "Missing namespace declaration")
            ("[<CLIMutable>]", "Missing CLIMutable attribute")
            ("type ", "No types generated")
        ]
        
        let errors =
            validations
            |> List.filter (fun (text, _) -> not (generatedCode.Contains(text)))
            |> List.map snd
        
        if errors.IsEmpty then
            Ok ()
        else
            Error errors