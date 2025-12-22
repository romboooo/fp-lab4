module ORM.ORMCore

open System
open Npgsql
open ORM.Database
open ORM.QueryBuilder
open ORM.DataMapper

type TableContext<'T> = {
    TableName: string
    Connection: DatabaseConnection
}

[<RequireQualifiedAccess>]
module TableContext =
    let create tableName connection : TableContext<'T> = {
        TableName = tableName
        Connection = connection
    }

[<RequireQualifiedAccess>]
module CRUD =
    let findAll (ctx: TableContext<'T>) : Result<'T list, string> =
        try
            let query = Query.select ctx.TableName
            let sql, parameters = SqlGenerator.generate query
            
            let results = 
                ctx.Connection.ExecuteReaderAction(sql, parameters, fun reader ->
                    mapDataReaderToRecords<'T> reader
                )
            Ok results
        with ex ->
            Error ex.Message
    
    let findBy (ctx: TableContext<'T>) (condition: Condition) : Result<'T list, string> =
        try
            let query =
                Query.select ctx.TableName
                |> Query.where condition
            
            let sql, parameters = SqlGenerator.generate query
            
            let results = 
                ctx.Connection.ExecuteReaderAction(sql, parameters, fun reader ->
                    mapDataReaderToRecords<'T> reader
                )
            Ok results
        with ex ->
            Error ex.Message
    
    let findById (ctx: TableContext<'T>) (id: int64) : Result<'T option, string> =
        try
            let query =
                Query.select ctx.TableName
                |> Query.where (Condition.equals "id" id)
                |> Query.limit 1
            
            let sql, parameters = SqlGenerator.generate query
            
            let results = 
                ctx.Connection.ExecuteReaderAction(sql, parameters, fun reader ->
                    mapDataReaderToRecords<'T> reader
                )
            
            match results with
            | [] -> Ok None
            | [record] -> Ok (Some record)
            | _ -> Ok (Some results.Head)
        with ex ->
            Error ex.Message
    
    
    let insert (ctx: TableContext<'T>) (values: (string * obj) list) : Result<int64, string> =
        try
            if List.isEmpty values then
                failwith "No fields to insert"
            
            let query =
                Query.insert ctx.TableName
                |> Query.values values
                |> Query.returning ["id"]
            
            let sql, parameters = SqlGenerator.generate query
            
            let result = ctx.Connection.ExecuteScalar(sql, parameters)
            
            match result with
            | :? int64 as id -> Ok id
            | :? int as id -> Ok (int64 id)
            | null -> Ok 0L
            | _ -> Ok 0L
            
        with ex ->
            Error ex.Message
    
    let insertAndReturn (ctx: TableContext<'T>) (values: (string * obj) list) : Result<'T option, string> =
        try
            if List.isEmpty values then
                failwith "No fields to insert"
            
            let query =
                Query.insert ctx.TableName
                |> Query.values values
                |> Query.returning ["*"]
            
            let sql, parameters = SqlGenerator.generate query
            
            let results = 
                ctx.Connection.ExecuteReaderAction(sql, parameters, fun reader ->
                    mapDataReaderToRecords<'T> reader
                )
            
            match results with
            | [] -> Ok None
            | [record] -> Ok (Some record)
            | _ -> Ok (Some results.Head)
            
        with ex ->
            Error ex.Message
    
    let update (ctx: TableContext<'T>) (id: int64) (setValues: (string * obj) list) : Result<int, string> =
        try
            if List.isEmpty setValues then
                failwith "No fields to update"
            
            let query =
                Query.update ctx.TableName
                |> Query.set setValues
                |> Query.where (Condition.equals "id" id)
            
            let sql, parameters = SqlGenerator.generate query
            
            let rowsAffected = ctx.Connection.ExecuteNonQuery(sql, parameters)
            Ok rowsAffected
        with ex ->
            Error ex.Message
    
    let updateWhere (ctx: TableContext<'T>) (condition: Condition) (setValues: (string * obj) list) : Result<int, string> =
        try
            if List.isEmpty setValues then
                failwith "No fields to update"
            
            let query =
                Query.update ctx.TableName
                |> Query.set setValues
                |> Query.where condition
            
            let sql, parameters = SqlGenerator.generate query
            
            let rowsAffected = ctx.Connection.ExecuteNonQuery(sql, parameters)
            Ok rowsAffected
        with ex ->
            Error ex.Message
    
    let delete (ctx: TableContext<'T>) (id: int64) : Result<int, string> =
        try
            let query =
                Query.delete ctx.TableName
                |> Query.where (Condition.equals "id" id)
            
            let sql, parameters = SqlGenerator.generate query
            
            let rowsAffected = ctx.Connection.ExecuteNonQuery(sql, parameters)
            Ok rowsAffected
        with ex ->
            Error ex.Message
    
    let deleteWhere (ctx: TableContext<'T>) (condition: Condition) : Result<int, string> =
        try
            let query =
                Query.delete ctx.TableName
                |> Query.where condition
            
            let sql, parameters = SqlGenerator.generate query
            
            let rowsAffected = ctx.Connection.ExecuteNonQuery(sql, parameters)
            Ok rowsAffected
        with ex ->
            Error ex.Message
    
    let executeRaw (ctx: TableContext<'T>) (sql: string) (parameters: (string * obj) list) : Result<int, string> =
        try
            let npgsqlParams = 
                parameters 
                |> List.mapi (fun i (name, value) -> NpgsqlParameter(name, value))
            
            let rowsAffected = ctx.Connection.ExecuteNonQuery(sql, npgsqlParams)
            Ok rowsAffected
        with ex ->
            Error ex.Message
    
    let executeScalar (ctx: TableContext<'T>) (sql: string) (parameters: (string * obj) list) : Result<obj, string> =
        try
            let npgsqlParams = 
                parameters 
                |> List.mapi (fun i (name, value) -> NpgsqlParameter(name, value))
            
            let result = ctx.Connection.ExecuteScalar(sql, npgsqlParams)
            Ok result
        with ex ->
            Error ex.Message

type DatabaseConnection with
    member this.Table<'T>(tableName: string) : TableContext<'T> =
        TableContext.create tableName this