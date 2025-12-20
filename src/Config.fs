// src/Config.fs - синглтон для конфигурации
namespace ORM.Config

open System

type DatabaseConfig = {
    ConnectionString: string
    Schema: string
}

module ConfigManager =
    let mutable private config: DatabaseConfig option = None
    
    let initialize (connString: string) (schema: string) =
        config <- Some { 
            ConnectionString = connString
            Schema = schema 
        }
    
    let getConfig() =
        match config with
        | Some cfg -> cfg
        | None -> failwith "Configuration not initialized. Call ConfigManager.initialize first."