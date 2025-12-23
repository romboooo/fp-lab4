module ORM.ORMCore

open System
open Npgsql
open ORM.Database
open ORM.QueryBuilder
open ORM.DataMapper


/// <summary>
/// Контекст таблицы для выполнения операций CRUD
/// </summary>
/// <typeparam name="'T">Тип записи таблицы</typeparam>
type TableContext<'T> =
    { TableName: string
      Connection: DatabaseConnection }

/// <summary>
/// Модуль для работы с TableContext
/// </summary>
[<RequireQualifiedAccess>]
module TableContext =
    /// <summary>
    /// Создает новый контекст таблицы
    /// </summary>
    /// <param name="tableName">Имя таблицы</param>
    /// <param name="connection">Подключение к базе данных</param>
    /// <returns>Контекст таблицы</returns>
    let create tableName connection : TableContext<'T> =
        { TableName = tableName
          Connection = connection }

/// <summary>
/// Модуль для выполнения операций CRUD
/// </summary>
[<RequireQualifiedAccess>]
module CRUD =
    /// <summary>
    /// Возвращает все записи из таблицы
    /// </summary>
    /// <param name="ctx">Контекст таблицы</param>
    /// <returns>Список записей или сообщение об ошибке</returns>
    let findAll (ctx: TableContext<'T>) : Result<'T list, string> =
        try
            let query = Query.select ctx.TableName
            let sql, parameters = SqlGenerator.generate query

            let results =
                ctx.Connection.ExecuteReaderAction(sql, parameters, fun reader -> mapDataReaderToRecords<'T> reader)

            Ok results
        with ex ->
            Error ex.Message

    /// <summary>
    /// Возвращает записи, соответствующие условию
    /// </summary>
    /// <param name="ctx">Контекст таблицы</param>
    /// <param name="condition">Условие для фильтрации</param>
    /// <returns>Список записей или сообщение об ошибке</returns>
    let findBy (ctx: TableContext<'T>) (condition: Condition) : Result<'T list, string> =
        try
            let query = Query.select ctx.TableName |> Query.where condition

            let sql, parameters = SqlGenerator.generate query

            let results =
                ctx.Connection.ExecuteReaderAction(sql, parameters, fun reader -> mapDataReaderToRecords<'T> reader)

            Ok results
        with ex ->
            Error ex.Message

    /// <summary>
    /// Возвращает запись по идентификатору
    /// </summary>
    /// <param name="ctx">Контекст таблицы</param>
    /// <param name="id">Идентификатор записи</param>
    /// <returns>Опциональная запись или сообщение об ошибке</returns>
    let findById (ctx: TableContext<'T>) (id: int64) : Result<'T option, string> =
        try
            let query =
                Query.select ctx.TableName
                |> Query.where (Condition.equals "id" id)
                |> Query.limit 1

            let sql, parameters = SqlGenerator.generate query

            let results =
                ctx.Connection.ExecuteReaderAction(sql, parameters, fun reader -> mapDataReaderToRecords<'T> reader)

            match results with
            | [] -> Ok None
            | [ record ] -> Ok(Some record)
            | _ -> Ok(Some results.Head)
        with ex ->
            Error ex.Message

    /// <summary>
    /// Вставляет новую запись в таблицу
    /// </summary>
    /// <param name="ctx">Контекст таблицы</param>
    /// <param name="values">Список пар (поле, значение) для вставки</param>
    /// <returns>Идентификатор вставленной записи или сообщение об ошибке</returns>
    let insert (ctx: TableContext<'T>) (values: (string * obj) list) : Result<int64, string> =
        try
            if List.isEmpty values then
                failwith "No fields to insert"

            let query =
                Query.insert ctx.TableName |> Query.values values |> Query.returning [ "id" ]

            let sql, parameters = SqlGenerator.generate query

            let result = ctx.Connection.ExecuteScalar(sql, parameters)

            match result with
            | :? int64 as id -> Ok id
            | :? int as id -> Ok(int64 id)
            | null -> Ok 0L
            | _ -> Ok 0L

        with ex ->
            Error ex.Message

    /// <summary>
    /// Вставляет запись и возвращает её
    /// </summary>
    /// <param name="ctx">Контекст таблицы</param>
    /// <param name="values">Список пар (поле, значение) для вставки</param>
    /// <returns>Вставленная запись или сообщение об ошибке</returns>
    let insertAndReturn (ctx: TableContext<'T>) (values: (string * obj) list) : Result<'T option, string> =
        try
            if List.isEmpty values then
                failwith "No fields to insert"

            let query =
                Query.insert ctx.TableName |> Query.values values |> Query.returning [ "*" ]

            let sql, parameters = SqlGenerator.generate query

            let results =
                ctx.Connection.ExecuteReaderAction(sql, parameters, fun reader -> mapDataReaderToRecords<'T> reader)

            match results with
            | [] -> Ok None
            | [ record ] -> Ok(Some record)
            | _ -> Ok(Some results.Head)

        with ex ->
            Error ex.Message

    /// <summary>
    /// Обновляет запись по идентификатору
    /// </summary>
    /// <param name="ctx">Контекст таблицы</param>
    /// <param name="id">Идентификатор записи</param>
    /// <param name="setValues">Список пар (поле, новое значение) для обновления</param>
    /// <returns>Количество обновленных строк или сообщение об ошибке</returns>
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

    /// <summary>
    /// Обновляет записи, соответствующие условию
    /// </summary>
    /// <param name="ctx">Контекст таблицы</param>
    /// <param name="condition">Условие для фильтрации</param>
    /// <param name="setValues">Список пар (поле, новое значение) для обновления</param>
    /// <returns>Количество обновленных строк или сообщение об ошибке</returns>
    let updateWhere
        (ctx: TableContext<'T>)
        (condition: Condition)
        (setValues: (string * obj) list)
        : Result<int, string> =
        try
            if List.isEmpty setValues then
                failwith "No fields to update"

            let query =
                Query.update ctx.TableName |> Query.set setValues |> Query.where condition

            let sql, parameters = SqlGenerator.generate query

            let rowsAffected = ctx.Connection.ExecuteNonQuery(sql, parameters)
            Ok rowsAffected
        with ex ->
            Error ex.Message

    /// <summary>
    /// Удаляет запись по идентификатору
    /// </summary>
    /// <param name="ctx">Контекст таблицы</param>
    /// <param name="id">Идентификатор записи</param>
    /// <returns>Количество удаленных строк или сообщение об ошибке</returns>
    let delete (ctx: TableContext<'T>) (id: int64) : Result<int, string> =
        try
            let query = Query.delete ctx.TableName |> Query.where (Condition.equals "id" id)

            let sql, parameters = SqlGenerator.generate query

            let rowsAffected = ctx.Connection.ExecuteNonQuery(sql, parameters)
            Ok rowsAffected
        with ex ->
            Error ex.Message

    /// <summary>
    /// Удаляет записи, соответствующие условию
    /// </summary>
    /// <param name="ctx">Контекст таблицы</param>
    /// <param name="condition">Условие для фильтрации</param>
    /// <returns>Количество удаленных строк или сообщение об ошибке</returns>
    let deleteWhere (ctx: TableContext<'T>) (condition: Condition) : Result<int, string> =
        try
            let query = Query.delete ctx.TableName |> Query.where condition

            let sql, parameters = SqlGenerator.generate query

            let rowsAffected = ctx.Connection.ExecuteNonQuery(sql, parameters)
            Ok rowsAffected
        with ex ->
            Error ex.Message

    /// <summary>
    /// Выполняет произвольный SQL-запрос
    /// </summary>
    /// <param name="ctx">Контекст таблицы</param>
    /// <param name="sql">SQL-запрос</param>
    /// <param name="parameters">Параметры запроса</param>
    /// <returns>Количество затронутых строк или сообщение об ошибке</returns>
    let executeRaw (ctx: TableContext<'T>) (sql: string) (parameters: (string * obj) list) : Result<int, string> =
        try
            let npgsqlParams =
                parameters |> List.mapi (fun i (name, value) -> NpgsqlParameter(name, value))

            let rowsAffected = ctx.Connection.ExecuteNonQuery(sql, npgsqlParams)
            Ok rowsAffected
        with ex ->
            Error ex.Message

    /// <summary>
    /// Выполняет произвольный SQL-запрос с возвратом скалярного значения
    /// </summary>
    /// <param name="ctx">Контекст таблицы</param>
    /// <param name="sql">SQL-запрос</param>
    /// <param name="parameters">Параметры запроса</param>
    /// <returns>Скалярное значение или сообщение об ошибке</returns>
    let executeScalar (ctx: TableContext<'T>) (sql: string) (parameters: (string * obj) list) : Result<obj, string> =
        try
            let npgsqlParams =
                parameters |> List.mapi (fun i (name, value) -> NpgsqlParameter(name, value))

            let result = ctx.Connection.ExecuteScalar(sql, npgsqlParams)
            Ok result
        with ex ->
            Error ex.Message

/// <summary>
/// Методы расширения для DatabaseConnection
/// </summary>
type DatabaseConnection with
    /// <summary>
    /// Получает контекст таблицы для работы с ней
    /// </summary>
    /// <param name="tableName">Имя таблицы</param>
    /// <returns>Контекст таблицы</returns>
    member this.Table<'T>(tableName: string) : TableContext<'T> = TableContext.create tableName this
