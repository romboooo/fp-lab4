module ORM.DataMapper

open System
open System.Data.Common
open Microsoft.FSharp.Reflection
open ORM  
/// <summary>
/// Модуль для работы с опциональными типами
/// </summary>
module OptionConverter =
    /// <summary>
    /// Пытается распаковать значение из Option типа
    /// </summary>
    /// <param name="value">Значение для распаковки</param>
    /// <returns>Some с внутренним значением или None</returns>
    let tryUnboxOption (value: obj) : obj option =
        if value = null || value = DBNull.Value then
            None
        else
            let t = value.GetType()
            if t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<_>> then
                let cases = FSharpType.GetUnionCases(t)
                let case, fields = FSharpValue.GetUnionFields(value, t)
                if case.Name = "Some" then Some fields.[0]
                else None
            else
                Some value
    /// <summary>
    /// Создает значение Option типа из внутреннего значения
    /// </summary>
    /// <param name="innerType">Тип внутреннего значения</param>
    /// <param name="value">Значение для обертывания</param>
    /// <returns>Значение Option типа</returns>
    let createOption (innerType: Type) (value: obj) : obj =
        if value = null || value = DBNull.Value then
            let noneCase = FSharpType.GetUnionCases(typedefof<option<_>>.MakeGenericType(innerType))
                            |> Array.find (fun c -> c.Name = "None")
            FSharpValue.MakeUnion(noneCase, [||])
        else
            let someCase = FSharpType.GetUnionCases(typedefof<option<_>>.MakeGenericType(innerType))
                            |> Array.find (fun c -> c.Name = "Some")
            FSharpValue.MakeUnion(someCase, [|value|])
/// <summary>
/// Модуль для преобразования типов значений
/// </summary>
module TypeConverter =
    /// <summary>
    /// Преобразует значение к целевому типу
    /// </summary>
    /// <param name="targetType">Целевой тип</param>
    /// <param name="value">Значение для преобразования</param>
    /// <returns>Преобразованное значение</returns>
     let convertValue (targetType: Type) (value: obj) : obj =
        if value = null || value = DBNull.Value then
            null
        else
            try
                match targetType with
                | t when t = typeof<int> -> 
                    match value with
                    | :? int64 as i64 -> Convert.ToInt32(i64) |> box
                    | :? int32 -> value
                    | :? int16 as i16 -> Convert.ToInt32(i16) |> box
                    | :? byte as b -> Convert.ToInt32(b) |> box
                    | _ -> Convert.ToInt32(value) |> box
                | t when t = typeof<int64> -> 
                    match value with
                    | :? int64 -> value
                    | :? int32 as i32 -> Convert.ToInt64(i32) |> box
                    | _ -> Convert.ToInt64(value) |> box
                | t when t = typeof<decimal> -> 
                    match value with
                    | :? decimal -> value
                    | :? double as d -> Convert.ToDecimal(d) |> box
                    | _ -> Convert.ToDecimal(value) |> box
                | t when t = typeof<float> -> 
                    match value with
                    | :? float -> value
                    | :? decimal as dec -> Convert.ToDouble(dec) |> box
                    | _ -> Convert.ToDouble(value) |> box
                | t when t = typeof<bool> -> 
                    match value with
                    | :? bool as b -> box b
                    | :? int64 as i64 -> (i64 <> 0L) |> box
                    | :? int32 as i32 -> (i32 <> 0) |> box
                    | :? int16 as i16 -> (i16 <> 0s) |> box
                    | :? byte as b -> (b <> 0uy) |> box
                    | :? string as s -> 
                        match Boolean.TryParse(s) with
                        | (true, b) -> box b
                        | _ -> Convert.ToBoolean(value) |> box
                    | _ -> Convert.ToBoolean(value) |> box
                | t when t = typeof<DateTime> -> 
                    match value with
                    | :? DateTime as dt -> box dt
                    | :? string as s -> 
                        match DateTime.TryParse(s) with
                        | (true, dt) -> box dt
                        | _ -> Convert.ToDateTime(value) |> box
                    | _ -> Convert.ToDateTime(value) |> box
                | t when t = typeof<string> -> 
                    match value with
                    | :? string -> value
                    | _ -> Convert.ToString(value) |> box
                | _ -> 
                    try
                        Convert.ChangeType(value, targetType)
                    with
                    | _ -> value
            with ex ->
                printfn "Warning: Failed to convert value %A to type %s: %s" value targetType.Name ex.Message
                value

/// <summary>
/// Преобразует DataReader в список записей F#
/// </summary>
/// <param name="reader">DataReader с результатами запроса</param>
/// <returns>Список записей типа 'T</returns>
/// <exception cref="System.Exception">Выбрасывается если тип не является записью F#</exception>
let mapDataReaderToRecords<'T> (reader: DbDataReader) : 'T list =
    let results = ResizeArray<'T>()
    
    if reader.HasRows then
        let recordType = typeof<'T>
        
        if not (FSharpType.IsRecord(recordType)) then
            failwithf "Type %s is not an F# record" recordType.Name
        
        let properties = FSharpType.GetRecordFields(recordType)
        
        let columnIndexMap =
            [0 .. reader.FieldCount - 1]
            |> List.map (fun i -> 
                let columnName = reader.GetName(i).ToLowerInvariant()
                columnName, i)
            |> Map.ofList
        
        while reader.Read() do
            let values = 
                properties
                |> Array.map (fun prop ->
                    let propName = prop.Name.ToLowerInvariant()
                    
                    match Map.tryFind propName columnIndexMap with
                    | Some columnIndex ->
                        if reader.IsDBNull(columnIndex) then
                            if prop.PropertyType.IsGenericType && 
                               prop.PropertyType.GetGenericTypeDefinition() = typedefof<option<_>> then
                                let innerType = prop.PropertyType.GetGenericArguments().[0]
                                OptionConverter.createOption innerType null
                            else
                                if prop.PropertyType.IsValueType then
                                    Activator.CreateInstance(prop.PropertyType)
                                else
                                    null
                        else
                            let dbValue = reader.GetValue(columnIndex)
                            
                            let convertedValue =
                                if prop.PropertyType.IsGenericType && 
                                   prop.PropertyType.GetGenericTypeDefinition() = typedefof<option<_>> then
                                    let innerType = prop.PropertyType.GetGenericArguments().[0]
                                    OptionConverter.createOption innerType (TypeConverter.convertValue innerType dbValue)
                                else
                                    TypeConverter.convertValue prop.PropertyType dbValue
                            
                            convertedValue
                    | None ->
                        let snakeCaseName = 
                            System.Text.RegularExpressions.Regex.Replace(
                                prop.Name, 
                                "(?<=.)([A-Z])", 
                                "_$1").ToLowerInvariant()
                        
                        match Map.tryFind snakeCaseName columnIndexMap with
                        | Some columnIndex ->
                            if reader.IsDBNull(columnIndex) then
                                if prop.PropertyType.IsGenericType && 
                                   prop.PropertyType.GetGenericTypeDefinition() = typedefof<option<_>> then
                                    let innerType = prop.PropertyType.GetGenericArguments().[0]
                                    OptionConverter.createOption innerType null
                                else
                                    if prop.PropertyType.IsValueType then
                                        Activator.CreateInstance(prop.PropertyType)
                                    else
                                        null
                            else
                                let dbValue = reader.GetValue(columnIndex)
                                let convertedValue =
                                    if prop.PropertyType.IsGenericType && 
                                       prop.PropertyType.GetGenericTypeDefinition() = typedefof<option<_>> then
                                        let innerType = prop.PropertyType.GetGenericArguments().[0]
                                        OptionConverter.createOption innerType (TypeConverter.convertValue innerType dbValue)
                                    else
                                        TypeConverter.convertValue prop.PropertyType dbValue
                                convertedValue
                        | None ->
                            if prop.PropertyType.IsGenericType && 
                               prop.PropertyType.GetGenericTypeDefinition() = typedefof<option<_>> then
                                let innerType = prop.PropertyType.GetGenericArguments().[0]
                                OptionConverter.createOption innerType null
                            else
                                if prop.PropertyType.IsValueType then
                                    Activator.CreateInstance(prop.PropertyType)
                                else
                                    null)
            
            try
                let record = FSharpValue.MakeRecord(recordType, values) :?> 'T
                results.Add(record)
            with ex ->
                printfn "Error creating record %s: %s" recordType.Name ex.Message
                printfn "Values: %A" values
                reraise()
    
    results |> Seq.toList
/// <summary>
/// Модуль для преобразования записей в параметры запросов
/// </summary>
module RecordConverter =
    open System.Reflection
    /// <summary>
    /// Получает свойство первичного ключа записи
    /// </summary>
    /// <returns>Опциональное свойство с атрибутом PrimaryKeyAttribute</returns>
    let getPrimaryKeyProperty<'T> () =
        typeof<'T>.GetProperties()
        |> Array.tryFind (fun prop -> 
            prop.GetCustomAttributes(typeof<PrimaryKeyAttribute>, true)
            |> Array.isEmpty
            |> not)  
            
    /// <summary>
    /// Преобразует запись в список параметров для запроса
    /// </summary>
    /// <param name="record">Запись для преобразования</param>
    /// <param name="includePrimaryKey">Включать ли первичный ключ в результат</param>
    /// <returns>Список пар (имя столбца в snake_case, значение)</returns>
    let recordToParameterList (record: 'T) (includePrimaryKey: bool) : (string * obj) list =
        let properties = typeof<'T>.GetProperties()
        
        properties
        |> Array.choose (fun prop ->
            let isPrimaryKey = 
                prop.GetCustomAttributes(typeof<PrimaryKeyAttribute>, true)
                |> Array.isEmpty
                |> not
            
            if not includePrimaryKey && isPrimaryKey then
                None
            else
                let value = prop.GetValue(record)
                
                let dbValue =
                    if value = null then
                        box DBNull.Value
                    elif prop.PropertyType.IsGenericType && 
                         prop.PropertyType.GetGenericTypeDefinition() = typedefof<option<_>> then
                        let cases = FSharpType.GetUnionCases(prop.PropertyType)
                        let case, fields = FSharpValue.GetUnionFields(value, prop.PropertyType)
                        if case.Name = "Some" then
                            fields.[0] 
                        else
                            box DBNull.Value
                    else
                        value
                
                let columnName = 
                    System.Text.RegularExpressions.Regex.Replace(
                        prop.Name, 
                        "(?<=.)([A-Z])", 
                        "_$1").ToLowerInvariant()
                
                Some (columnName, dbValue))
        |> Array.toList