namespace ORM.Schema

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
}

type TableInfo = {
    Schema: string
    Name: string
    Columns: ColumnInfo list
}

module TypeMapping = 
    let toFSharpType (col: ColumnInfo) =
        let baseType = 
            match col.DataType with
            | Int -> "int"
            | BigInt -> "int64"
            | Varchar _ | Text | Json -> "string" // wip
            | Boolean -> "bool"
            | Date -> "System.DateTime"
        
        if col.IsNullable && baseType <> "string"
        then baseType + " option"
        else baseType