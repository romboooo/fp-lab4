// src/core/ORMCore.fs
module ORM.ORMCore

open System
open System.Data.Common
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
    let insert<'T> (ctx: TableContext<'T>) (values: (string * obj) list) =
        try
            let query =
                Query.insert ctx.TableName
                |> Query.values values
                |> Query.returning ["id"]
            
            let sql, parameters = SqlGenerator.generate query
            
            let result = ctx.Connection.ExecuteScalar(sql, parameters)
            Ok result
            
        with ex ->
            Error ex.Message
    let findAll<'T> (ctx: TableContext<'T>) =
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
    
    let findBy<'T> (ctx: TableContext<'T>) (condition: Condition) =
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
    
    let findById<'T> (ctx: TableContext<'T>) (id: int) =
        try
            let query =
                Query.select ctx.TableName
                |> Query.where (Condition.equals "id" id)
            
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
    
    let update (ctx: TableContext<'T>) (id: int) (values: (string * obj) list) =
        try
            if List.isEmpty values then
                failwith "Нет полей для обновления"
            
            let query =
                Query.update ctx.TableName
                |> Query.set values
                |> Query.where (Condition.equals "id" id)
            
            let sql, parameters = SqlGenerator.generate query
            
            let rowsAffected = ctx.Connection.ExecuteNonQuery(sql, parameters)
            Ok rowsAffected
            
        with ex ->
            Error ex.Message
    
    let delete (ctx: TableContext<'T>) (id: int) =
        try
            let query =
                Query.delete ctx.TableName
                |> Query.where (Condition.equals "id" id)
            
            let sql, parameters = SqlGenerator.generate query
            
            let rowsAffected = ctx.Connection.ExecuteNonQuery(sql, parameters)
            Ok rowsAffected
            
        with ex ->
            Error ex.Message


type DatabaseConnection with
    member this.Table<'T>(tableName: string) =
        TableContext.create tableName this