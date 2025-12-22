module ORM.DataMapper

open System
open System.Data.Common
open Microsoft.FSharp.Reflection
open ORM  

module OptionConverter =
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

    let createOption (innerType: Type) (value: obj) : obj =
        if value = null || value = DBNull.Value then
            let noneCase = FSharpType.GetUnionCases(typedefof<option<_>>.MakeGenericType(innerType))
                            |> Array.find (fun c -> c.Name = "None")
            FSharpValue.MakeUnion(noneCase, [||])
        else
            let someCase = FSharpType.GetUnionCases(typedefof<option<_>>.MakeGenericType(innerType))
                            |> Array.find (fun c -> c.Name = "Some")
            FSharpValue.MakeUnion(someCase, [|value|])

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
                                match dbValue with
                                | :? int as i when prop.PropertyType = typeof<int> -> box i
                                | :? int64 as i when prop.PropertyType = typeof<int64> -> box i
                                | :? string as s when prop.PropertyType = typeof<string> -> box s
                                | :? bool as b when prop.PropertyType = typeof<bool> -> box b
                                | :? DateTime as d when prop.PropertyType = typeof<DateTime> -> box d
                                | :? decimal as d when prop.PropertyType = typeof<decimal> -> box d
                                | :? float as f when prop.PropertyType = typeof<float> -> box f
                                | _ -> dbValue // fallback
                            
                            if prop.PropertyType.IsGenericType && 
                               prop.PropertyType.GetGenericTypeDefinition() = typedefof<option<_>> then
                                let innerType = prop.PropertyType.GetGenericArguments().[0]
                                OptionConverter.createOption innerType convertedValue
                            else
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

module RecordConverter =
    open System.Reflection
    
    let getPrimaryKeyProperty<'T> () =
        typeof<'T>.GetProperties()
        |> Array.tryFind (fun prop -> 
            prop.GetCustomAttributes(typeof<PrimaryKeyAttribute>, true)
            |> Array.isEmpty
            |> not)  // Используем типизированную проверку
    
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
                
                Some (prop.Name.ToLower(), dbValue))
        |> Array.toList
module TypeConverter =
    let private convertValue (targetType: Type) (value: obj) : obj =
        if value = null || value = DBNull.Value then
            null
        else
            try
                match targetType with
                | t when t = typeof<int> -> Convert.ToInt32(value) |> box
                | t when t = typeof<int64> -> Convert.ToInt64(value) |> box
                | t when t = typeof<decimal> -> Convert.ToDecimal(value) |> box
                | t when t = typeof<float> -> Convert.ToDouble(value) |> box  
                | t when t = typeof<bool> -> Convert.ToBoolean(value) |> box
                | t when t = typeof<DateTime> -> Convert.ToDateTime(value) |> box
                | t when t = typeof<string> -> Convert.ToString(value) |> box
                | _ -> 
                    Convert.ChangeType(value, targetType)
            with ex ->
                printfn "Warning: Failed to convert value %A to type %s: %s" value targetType.Name ex.Message
                value
    