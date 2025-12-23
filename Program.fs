open System
open System.IO
open ORM.Database
open ORM.SqlParser
open ORM.TypeGenerator
open ORM.QueryBuilder
open ORM.ORMCore
open ORM.GeneratedTypes

[<EntryPoint>]
let main argv =
    if argv |> Array.contains "--generate-types" then
        printfn "Генерация F# типов из схемы базы данных..."
        
        match Generator.generateTypes() with
        | Ok generatedCode ->
            match Generator.validateGeneratedTypes generatedCode with
            | Ok _ -> 
                let lines = generatedCode.Split('\n')
                let typesCount = lines |> Array.filter (fun l -> l.Trim().StartsWith("type ")) |> Array.length
                printfn "Сгенерировано типов: %d" typesCount
                printfn "Сохранено в: %s" (Path.GetFullPath("generated/GeneratedTypes.fs"))
                0
            | Error errors ->
                printfn "Ошибка валидации:"
                errors |> List.iter (printfn "  - %s")
                1
        | Error err ->
            printfn "Ошибка генерации: %s" err
            1
    else 
        0