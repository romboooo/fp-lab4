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