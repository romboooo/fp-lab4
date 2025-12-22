// src/Schema.fs
namespace ORM.Schema

open System

type ColumnType =
    | Int
    | BigInt
    | Varchar of maxLength: int option  
    | Text                           
    | Boolean
    | Json                            
    | Date
    | Float  

type ColumnInfo = {
    Name: string
    DataType: ColumnType
    IsNullable: bool
    IsPrimaryKey: bool
    MaxLength: int option
}

type TableInfo = {
    Schema: string
    Name: string
    Columns: ColumnInfo list
}

[<RequireQualifiedAccess>]
module TypeMapping =
    let pgTypeToFSharp (colType: ColumnType) (isNullable: bool) : string =
        let baseType =
            match colType with
            | Int -> "int"
            | BigInt -> "int64"
            | Varchar _ | Text | Json -> "string"
            | Boolean -> "bool"
            | Date -> "System.DateTime"
            | Float -> "float" 
        
        if isNullable then
            baseType + " option"
        else
            baseType