namespace ORM.Schema

/// <summary>
/// Типы столбцов PostgreSQL, поддерживаемые ORM
/// </summary>
type ColumnType =
    | Int
    | BigInt
    | Varchar of maxLength: int option
    | Text
    | Boolean
    | Json
    | Date
    | Float

/// <summary>
/// Информация о столбце таблицы
/// </summary>
type ColumnInfo =
    { Name: string
      DataType: ColumnType
      IsNullable: bool
      IsPrimaryKey: bool
      MaxLength: int option }

/// <summary>
/// Информация о таблице базы данных
/// </summary>
type TableInfo =
    { Schema: string
      Name: string
      Columns: ColumnInfo list }

/// <summary>
/// Модуль для маппинга типов PostgreSQL на F# типы
/// </summary>
[<RequireQualifiedAccess>]
module TypeMapping =
    /// <summary>
    /// Преобразует тип PostgreSQL в соответствующий тип F#
    /// </summary>
    /// <param name="colType">Тип столбца PostgreSQL</param>
    /// <param name="isNullable">Может ли содержать NULL</param>
    /// <returns>Строковое представление типа F#</returns>
    let pgTypeToFSharp (colType: ColumnType) (isNullable: bool) : string =
        let baseType =
            match colType with
            | Int -> "int"
            | BigInt -> "int64"
            | Varchar _
            | Text
            | Json -> "string"
            | Boolean -> "bool"
            | Date -> "System.DateTime"
            | Float -> "float"

        if isNullable then baseType + " option" else baseType
