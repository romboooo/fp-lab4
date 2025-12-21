open System
open System.IO
open ORM.Database
open ORM.SqlParser
open ORM.TypeGenerator

[<EntryPoint>]
let main argv =
    if argv |> Array.contains "--generate-types" then
        printfn "Generating F# types..."
        
        let outputPath = Path.Combine(Directory.GetCurrentDirectory(), "src", "GeneratedTypes.fs")
        
        match Generator.generateTypes outputPath with
        | Ok generatedCode ->
            match Generator.validateGeneratedTypes generatedCode with
            | Ok _ -> 
                printfn "Type validation passed!"
                printfn "Generated file: %s" outputPath
                0
            | Error errors ->
                printfn "Type validation failed:"
                errors |> List.iter (printfn "  - %s")
                1
        | Error err ->
            printfn "Error generating types: %s" err
            1
    else
        try
            let db = DatabaseConnection()
            
            printfn "1. Testing database connection..."
            use conn = db.GetOpenConnection()
            printfn "   Connected to: %s" conn.Database
            printfn "   Connection state: %s" (conn.State.ToString())
            conn.Close()
            
            printfn "\n2. Parsing database schema..."
            Parser.generateSchemaSummary()
            
            printfn "\n3. Detailed schema information:"
            Parser.printDatabaseSchema()
            
            0
        with ex ->
            printfn "\nError: %s" ex.Message
            1