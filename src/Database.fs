module ORM.Database

open Npgsql
open System
open dotenv.net

type ConnectionConfig = {
    Host: string
    Port: int
    Database: string
    Username: string
    Password: string
}

module Config =
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
    
    let buildConnectionString (config: ConnectionConfig) =
        sprintf "Host=%s;Port=%d;Database=%s;Username=%s;Password=%s;Include Error Detail=true" 
            config.Host config.Port config.Database config.Username config.Password

type DatabaseConnection() =
    let config = Config.load()
    let connectionString = Config.buildConnectionString config
    
    member this.GetOpenConnection() =
        let conn = new NpgsqlConnection(connectionString)
        conn.Open()
        conn
    
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