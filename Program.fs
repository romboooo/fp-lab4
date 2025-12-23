open System
open System.IO
open ORM.Database
open ORM.SqlParser
open ORM.TypeGenerator
open ORM.QueryBuilder
open ORM.ORMCore
open ORM.GeneratedTypes

let runRealExample () =
    try
        use db = new DatabaseConnection()
        let usersTable = db.Table<Users>("users")
        let productsTable = db.Table<Products>("products")
        let ordersTable = db.Table<Orders>("orders")
        
        let testUserId = ref 0L
        let testProductId = ref 0L
        let testOrderId = ref 0L
        
        printfn "\n1. Создание тестового пользователя..."
        let testUsername = $"test_user_{DateTime.Now:yyyyMMddHHmmss}"
        let testEmail = $"test_{DateTime.Now:yyyyMMddHHmmss}@example.com"
        
        match CRUD.insert usersTable [
            "username", box testUsername
            "email", box testEmail
            "age", box 25
        ] with
        | Ok userId ->
            testUserId := userId
            printfn "   Создан пользователь с ID: %d" userId
            
            printfn "\n2. Получение созданного пользователя..."
            match CRUD.findById usersTable userId with
            | Ok (Some user) ->
                printfn "   Найден пользователь: %s (%s), возраст: %A" user.username user.email user.age
                
                printfn "\n3. Поиск пользователей с email содержащим 'example.com'..."
                match CRUD.findBy usersTable (Condition.like "email" "%example.com%") with
                | Ok users -> 
                    printfn "   Найдено пользователей: %d" users.Length
                | Error err -> printfn "   Ошибка: %s" err
                
                printfn "\n4. Создание тестового продукта..."
                let productName = $"Test Product {DateTime.Now:HHmmss}"
                
                match CRUD.insert productsTable [
                    "name", box productName
                    "price", box 9999
                    "category", box "Electronics"
                    "in_Stock", box true
                ] with
                | Ok productId ->
                    testProductId := productId
                    printfn "   Создан продукт с ID: %d" productId
                    
                    printfn "\n5. Получение созданного продукта..."
                    match CRUD.findById productsTable productId with
                    | Ok (Some product) ->
                        printfn "   Найден продукт: %s, цена: %d, категория: %A" product.name product.price product.category
                        
                        printfn "\n6. Обновление цены продукта..."
                        match CRUD.update productsTable productId ["price", box 7999] with
                        | Ok rowsUpdated ->
                            printfn "   Обновлено строк: %d" rowsUpdated
                            
                            printfn "\n7. Создание заказа..."
                            match CRUD.insert ordersTable [
                                "user_id", box userId
                                "product_id", box productId
                                "quantity", box 2
                                "status", box "pending"
                            ] with
                            | Ok orderId ->
                                testOrderId := orderId
                                printfn "   Создан заказ с ID: %d" orderId
                                
                                printfn "\n8. Получение созданного заказа..."
                                match CRUD.findById ordersTable orderId with
                                | Ok (Some order) ->
                                    printfn "   Найден заказ: статус - %A, количество - %A" order.status order.quantity
                                    
                                    printfn "\n9. Поиск всех заказов пользователя..."
                                    match CRUD.findBy ordersTable (Condition.equals "user_id" userId) with
                                    | Ok userOrders ->
                                        printfn "   Найдено заказов пользователя: %d" userOrders.Length
                                        
                                        printfn "\n10. Обновление статуса заказа..."
                                        match CRUD.update ordersTable orderId ["status", box "completed"] with
                                        | Ok _ ->
                                            printfn "   Статус заказа обновлен"
                                            
                                            printfn "\n11. Получение всех продуктов в категории 'Electronics'..."
                                            match CRUD.findBy productsTable (Condition.equals "category" "Electronics") with
                                            | Ok electronicsProducts ->
                                                printfn "   Найдено продуктов в категории Electronics: %d" electronicsProducts.Length
                                                
                                                printfn "\n12. Получение пользователей старше 20 лет..."
                                                match CRUD.findBy usersTable (Condition.greaterThan "age" 20) with
                                                | Ok adultUsers ->
                                                    printfn "   Найдено пользователей старше 20 лет: %d" adultUsers.Length
                                                    
                                                    printfn "\n13. Проверка сохранности тестовых данных в БД:"
                                                    
                                                    let checkUser = CRUD.findById usersTable !testUserId
                                                    let checkProduct = CRUD.findById productsTable !testProductId
                                                    let checkOrder = CRUD.findById ordersTable !testOrderId
                                                    
                                                    match checkUser, checkProduct, checkOrder with
                                                    | Ok (Some user), Ok (Some product), Ok (Some order) ->
                                                        printfn "   ✓ Пользователь %s найден в БД" user.username
                                                        printfn "   ✓ Продукт %s найден в БД" product.name
                                                        printfn "   ✓ Заказ #%d найден в БД" order.id
                                                        0
                                                    | _ ->
                                                        printfn "   ✗ Не все тестовые данные найдены в БД"
                                                        1
                                                | Error err -> 
                                                    printfn "   Ошибка при получении пользователей: %s" err
                                                    1
                                            | Error err -> 
                                                printfn "   Ошибка при получении продуктов: %s" err
                                                1
                                        | Error err -> 
                                            printfn "   Ошибка при обновлении заказа: %s" err
                                            1
                                    | Error err -> 
                                        printfn "   Ошибка при поиске заказов: %s" err
                                        1
                                | Ok None -> 
                                    printfn "   Заказ не найден"
                                    1
                                | Error err -> 
                                    printfn "   Ошибка при получении заказа: %s" err
                                    1
                            | Error err -> 
                                printfn "   Ошибка при создании заказа: %s" err
                                1
                        | Error err -> 
                            printfn "   Ошибка при обновлении продукта: %s" err
                            1
                    | Ok None -> 
                        printfn "   Продукт не найден"
                        1
                    | Error err -> 
                        printfn "   Ошибка при получении продукта: %s" err
                        1
                | Error err -> 
                    printfn "   Ошибка при создании продукта: %s" err
                    1
            | Ok None -> 
                printfn "   Пользователь не найден"
                1
            | Error err -> 
                printfn "   Ошибка при получении пользователя: %s" err
                1
        | Error err -> 
            printfn "   Ошибка при создании пользователя: %s" err
            1
        
    with ex ->
        printfn "Ошибка: %s" ex.Message
        1

let cleanupTestData () =
    printfn "=== Очистка тестовых данных ==="
    
    try
        use db = new DatabaseConnection()
        let usersTable = db.Table<Users>("users")
        let productsTable = db.Table<Products>("products")
        let ordersTable = db.Table<Orders>("orders")
        
        match CRUD.deleteWhere usersTable (Condition.like "username" "test_user_%") with
        | Ok count -> printfn "Удалено тестовых пользователей: %d" count
        | Error err -> printfn "Ошибка при удалении пользователей: %s" err
        
        match CRUD.deleteWhere productsTable (Condition.like "name" "Test Product%") with
        | Ok count -> printfn "Удалено тестовых продуктов: %d" count
        | Error err -> printfn "Ошибка при удалении продуктов: %s" err
        
        match CRUD.deleteWhere ordersTable (Condition.equals "status" "pending") with
        | Ok count -> printfn "Удалено тестовых заказов: %d" count
        | Error err -> printfn "Ошибка при удалении заказов: %s" err
        
        match CRUD.deleteWhere ordersTable (Condition.equals "status" "completed") with
        | Ok count -> printfn "Удалено выполненных заказов: %d" count
        | Error err -> printfn "Ошибка при удалении заказов: %s" err
        
        0
    with ex ->
        printfn "Ошибка при очистке: %s" ex.Message
        1


let runSchemaDemo () =
    printfn "=== Информация о схеме базы данных ==="
    try
        let db = new DatabaseConnection()
        use conn = db.GetOpenConnection()
        printfn "Подключено к базе: %s" conn.Database
        conn.Close()
        
        printfn "\nСтруктура базы данных:"
        Parser.printDatabaseSchema()
        
        printfn "\nСводка:"
        Parser.generateSchemaSummary()
        0
    with ex ->
        printfn "Ошибка при подключении к базе: %s" ex.Message
        1

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
    
    elif argv |> Array.contains "--show-schema" then
        runSchemaDemo()
    
    elif argv |> Array.contains "--example" then
        runRealExample()
    
    elif argv |> Array.contains "--cleanup-test-data" then
        cleanupTestData()
    
    else
        printfn "F# Type-Safe ORM"
        printfn ""
        printfn "Основные команды:"
        printfn "  dotnet run -- --generate-types    Сгенерировать F# типы из схемы БД"
        printfn "  dotnet run -- --show-schema       Показать структуру базы данных"
        printfn "  dotnet run -- --example           Реальный пример использования ORM"
        printfn "  dotnet run -- --cleanup-test-data Очистить тестовые данные"
        printfn ""
        printfn "Для запуска тестов: dotnet test"
        printfn ""
        
        0




// [<EntryPoint>]
// let main argv =
//     if argv |> Array.contains "--generate-types" then
//         printfn "Генерация F# типов из схемы базы данных..."
        
//         match Generator.generateTypes() with
//         | Ok generatedCode ->
//             match Generator.validateGeneratedTypes generatedCode with
//             | Ok _ -> 
//                 let lines = generatedCode.Split('\n')
//                 let typesCount = lines |> Array.filter (fun l -> l.Trim().StartsWith("type ")) |> Array.length
//                 printfn "Сгенерировано типов: %d" typesCount
//                 printfn "Сохранено в: %s" (Path.GetFullPath("generated/GeneratedTypes.fs"))
//                 0
//             | Error errors ->
//                 printfn "Ошибка валидации:"
//                 errors |> List.iter (printfn "  - %s")
//                 1
//         | Error err ->
//             printfn "Ошибка генерации: %s" err
//             1
//     else 
//         0