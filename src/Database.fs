module ORM.Database

open Npgsql
open System
open System.Data.Common
open dotenv.net

/// <summary>
/// Конфигурация подключения к PostgreSQL
/// </summary>
type ConnectionConfig = {
    Host: string
    Port: int
    Database: string
    Username: string
    Password: string
}
/// <summary>
/// Модуль для работы с конфигурацией подключения
/// </summary>
module Config =

    /// <summary>
    /// Загружает конфигурацию из переменных окружения
    /// </summary>
    /// <returns>Конфигурация подключения</returns>
    /// <exception cref="System.Exception">Выбрасывается если переменные окружения не установлены</exception>
    let load() =
        DotEnv.Load()

        let host = System.Environment.GetEnvironmentVariable("POSTGRES_HOST")
        let port = System.Environment.GetEnvironmentVariable("POSTGRES_PORT")
        let database = System.Environment.GetEnvironmentVariable("POSTGRES_DB")
        let username = System.Environment.GetEnvironmentVariable("POSTGRES_USER")
        let password = System.Environment.GetEnvironmentVariable("POSTGRES_PASSWORD")
        
        if String.IsNullOrEmpty(host) then 
            failwith "POSTGRES_HOST environment variable is not set"
        if String.IsNullOrEmpty(port) then 
            failwith "POSTGRES_PORT environment variable is not set"
        if String.IsNullOrEmpty(database) then 
            failwith "POSTGRES_DB environment variable is not set"
        if String.IsNullOrEmpty(username) then 
            failwith "POSTGRES_USER environment variable is not set"
        if String.IsNullOrEmpty(password) then 
            failwith "POSTGRES_PASSWORD environment variable is not set"
        
        {
            Host = host
            Port = Int32.Parse(port)
            Database = database
            Username = username
            Password = password
        }
    /// <summary>
    /// Строит строку подключения из конфигурации
    /// </summary>
    /// <param name="config">Конфигурация подключения</param>
    /// <returns>Строка подключения PostgreSQL</returns>
    let buildConnectionString (config: ConnectionConfig) =
        sprintf "Host=%s;Port=%d;Database=%s;Username=%s;Password=%s;Include Error Detail=true" 
            config.Host config.Port config.Database config.Username config.Password
/// <summary>
/// Основной тип для работы с подключением к базе данных
/// </summary>
/// <remarks>
/// Предоставляет методы для выполнения SQL-запросов и управления подключением
/// </remarks>
type DatabaseConnection() =
    let config = Config.load()
    let connectionString = Config.buildConnectionString config
    
    interface System.IDisposable with
        member this.Dispose() =
            () 

    /// <summary>
    /// Создает и открывает новое подключение к базе данных
    /// </summary>
    /// <returns>Открытое подключение NpgsqlConnection</returns>
    member this.GetOpenConnection() =
        let conn = new NpgsqlConnection(connectionString)
        conn.Open()
        conn
    
    /// <summary>
    /// Выполняет SQL-запрос и выводит результаты в консоль
    /// </summary>
    /// <param name="sql">SQL-запрос для выполнения</param>
    member this.ExecuteQuery (sql: string) =
        use conn = this.GetOpenConnection()
        use cmd = new NpgsqlCommand(sql, conn)
        use reader = cmd.ExecuteReader()
        
        while reader.Read() do
            for i = 0 to reader.FieldCount - 1 do
                let columnName = reader.GetName(i)
                let value = 
                    if reader.IsDBNull(i) then "NULL" 
                    else reader.GetValue(i).ToString()
                printf "%s: %s, " columnName value
            printfn ""
        
        printfn "Query executed successfully"

    /// <summary>
    /// Выполняет SQL-запрос без возврата результата (INSERT, UPDATE, DELETE)
    /// </summary>
    /// <param name="sql">SQL-запрос для выполнения</param>
    /// <param name="parameters">Список параметров запроса</param>
    /// <returns>Количество затронутых строк</returns>
    member this.ExecuteNonQuery(sql: string, parameters: NpgsqlParameter list) =
        use conn = this.GetOpenConnection()
        use cmd = new NpgsqlCommand(sql, conn)
        cmd.Parameters.AddRange(parameters |> List.toArray)
        cmd.ExecuteNonQuery()

    /// <summary>
    /// Выполняет SQL-запрос и возвращает скалярное значение
    /// </summary>
    /// <param name="sql">SQL-запрос для выполнения</param>
    /// <param name="parameters">Список параметров запроса</param>
    /// <returns>Скалярное значение результата</returns>
    member this.ExecuteScalar(sql: string, parameters: NpgsqlParameter list) =
        use conn = this.GetOpenConnection()
        use cmd = new NpgsqlCommand(sql, conn)
        cmd.Parameters.AddRange(parameters |> List.toArray)
        cmd.ExecuteScalar()

    /// <summary>
    /// Выполняет SQL-запрос с обработкой результата через функцию
    /// </summary>
    /// <param name="sql">SQL-запрос для выполнения</param>
    /// <param name="parameters">Список параметров запроса</param>
    /// <param name="action">Функция для обработки DataReader</param>
    /// <returns>Результат обработки функцией action</returns>
    member this.ExecuteReaderAction(sql: string, parameters: NpgsqlParameter list, action: DbDataReader -> 'T) =
        use conn = this.GetOpenConnection()
        use cmd = new NpgsqlCommand(sql, conn)
        cmd.Parameters.AddRange(parameters |> List.toArray)
        use reader = cmd.ExecuteReader()
        action reader
        
    /// <summary>
    /// Выполняет операции в транзакции
    /// </summary>
    /// <param name="action">Действие для выполнения в транзакции</param>
    /// <returns>Результат действия или ошибку</returns>
    member this.WithTransaction (action: NpgsqlConnection -> 'T) =
        use conn = this.GetOpenConnection()
        use transaction = conn.BeginTransaction()
        
        try
            let result = action conn
            transaction.Commit()
            Ok result
        with ex ->
            transaction.Rollback()
            Error ex.Message