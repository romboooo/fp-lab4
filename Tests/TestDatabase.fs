namespace ORM.Tests

open System
open System.Data
open Microsoft.Data.Sqlite
open ORM.DataMapper

module TestDatabase =
    
    type TestTable = {
        Id: int
        Name: string
        Age: int option
        CreatedAt: DateTime option
        Price: decimal option
    }
    
    let createInMemoryDatabase () =
        let connection = new SqliteConnection("DataSource=:memory:")
        connection.Open()
        
        let createTableSql = """
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                age INTEGER,
                created_at DATETIME,
                price DECIMAL(10,2)
            )
        """
        
        use cmd = new SqliteCommand(createTableSql, connection)
        cmd.ExecuteNonQuery() |> ignore
        
        connection
    
    let insertTestData (connection: SqliteConnection) =
        let testData = [
            "INSERT INTO test_table (name, age, created_at, price) VALUES ('Alice', 25, '2023-01-01', 99.99)"
            "INSERT INTO test_table (name, age, created_at, price) VALUES ('Bob', NULL, '2023-02-01', NULL)"
            "INSERT INTO test_table (name, age, created_at, price) VALUES ('Charlie', 30, NULL, 149.99)"
        ]
        
        for sql in testData do
            use cmd = new SqliteCommand(sql, connection)
            cmd.ExecuteNonQuery() |> ignore
    
    let executeQuery (connection: SqliteConnection) (sql: string) =
        use cmd = new SqliteCommand(sql, connection)
        use reader = cmd.ExecuteReader()
        mapDataReaderToRecords<TestTable> reader
