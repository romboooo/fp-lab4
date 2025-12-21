module ORM.DataMapper

open System
open System.Data.Common
open Npgsql
open Microsoft.FSharp.Reflection

let mapDataReaderToRecords<'T> (reader: DbDataReader) =
    let results = System.Collections.Generic.List<'T>()
    
    if reader.HasRows then
        let recordType = typeof<'T>
        
        if not (FSharpType.IsRecord(recordType)) then
            failwithf "Тип %s не является записью F#" recordType.Name
        
        let properties = FSharpType.GetRecordFields(recordType)
        
        while reader.Read() do
            let values = 
                properties
                |> Array.map (fun prop ->
                    let columnName = prop.Name.ToLower()
                    try
                        let columnIndex = 
                            try reader.GetOrdinal(columnName) with _ -> -1
                        
                        if columnIndex = -1 || reader.IsDBNull(columnIndex) then
                            // Дчля option типов возвращаем None
                            if prop.PropertyType.IsGenericType && 
                               prop.PropertyType.GetGenericTypeDefinition() = typedefof<Option<_>> then
                                let innerType = prop.PropertyType.GetGenericArguments().[0]
                                let optionType = typedefof<Option<_>>.MakeGenericType(innerType)
                                Activator.CreateInstance(optionType, [|null|])
                            else
                                null
                        else
                            let value = reader.GetValue(columnIndex)
                            if prop.PropertyType.IsGenericType && 
                               prop.PropertyType.GetGenericTypeDefinition() = typedefof<Option<_>> then
                                let innerType = prop.PropertyType.GetGenericArguments().[0]
                                let optionType = typedefof<Option<_>>.MakeGenericType(innerType)
                                Activator.CreateInstance(optionType, [|value|])
                            else
                                value
                    with ex ->
                        printfn "Ошибка чтения колонки %s: %s" columnName ex.Message
                        null)
            
            let record = FSharpValue.MakeRecord(recordType, values) :?> 'T
            results.Add(record)
    
    results |> Seq.toList