open System
open ORM.Database

[<EntryPoint>]
let main argv =
    printfn "=== Simple PostgreSQL Connection Test ===\n"
    
    try
        let db = DatabaseConnection()
        
        printfn "1. Testing connection..."
        use conn = db.GetOpenConnection()
        printfn "Connected to: %s" conn.Database
        printfn "Connection state: %s" (conn.State.ToString())
        conn.Close()
        
        printfn "\n2. Testing simple query..."
        db.ExecuteQuery "SELECT id, username, email FROM users LIMIT 5"
        
        printfn "\n3. Testing products query..."
        db.ExecuteQuery "SELECT id, name, price FROM products LIMIT 5"
        
        printfn "\n4. Testing orders query..."
        db.ExecuteQuery "SELECT id, user_id, product_id, status FROM orders LIMIT 5"
        
        printfn "\nAll tests passed!"
        0
        
    with ex ->
        printfn "\nError: %s" ex.Message
        1