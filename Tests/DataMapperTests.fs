namespace ORM.Tests

open System
open System.Data.Common
open Xunit
open ORM
open ORM.DataMapper
open Microsoft.Data.Sqlite

module DataMapperTests =

    type TestRecord = {
        [<PrimaryKey>] Id: int
        Name: string
        Age: int option
        CreatedAt: DateTime option
        Price: decimal option
        Score: float option
        IsActive: bool
        IsActiveOpt: bool option
    }

    type ComplexRecord = {
        [<PrimaryKey>] Id: int
        Name: string
        Age: int option
        Salary: decimal option
        IsActive: bool
        CreatedAt: DateTime
        UpdatedAt: DateTime option
        Score: float option
    }

    type TextRecord = {
        Id: int
        Name: string
        Description: string option
        Email: string
    }

    type BoolRecord = {
        Id: int
        Flag: bool option
        IsValid: bool
        IsActive: bool option
    }

    [<Fact>]
    let ``DataMapper should handle empty result set`` () =
        let connection = new SqliteConnection("DataSource=:memory:")
        connection.Open()
        
        let createTableSql = "CREATE TABLE empty_table (id INTEGER)"
        use cmd = new SqliteCommand(createTableSql, connection)
        cmd.ExecuteNonQuery() |> ignore
        
        let selectCmd = new SqliteCommand("SELECT * FROM empty_table", connection)
        use reader = selectCmd.ExecuteReader() :> DbDataReader
        
        let results = mapDataReaderToRecords<TestRecord> reader
        Assert.Empty(results)
    
    [<Fact>]
    let ``RecordConverter should convert records to parameters correctly`` () =
        let record = {
            Id = 1
            Name = "Test"
            Age = Some 30
            CreatedAt = Some DateTime.Now
            Price = Some 99.99m
            Score = Some 85.5
            IsActive = true
            IsActiveOpt = Some false
        }
        
        let paramsWithPk = RecordConverter.recordToParameterList record true
        Assert.Equal(8, paramsWithPk.Length)
        Assert.Contains(("id", box 1), paramsWithPk)
        Assert.Contains(("name", box "Test"), paramsWithPk)
        Assert.Contains(("age", box 30), paramsWithPk)
        
        let paramsWithoutPk = RecordConverter.recordToParameterList record false
        Assert.Equal(7, paramsWithoutPk.Length)
        Assert.DoesNotContain(("id", box 1), paramsWithoutPk)
        Assert.Contains(("name", box "Test"), paramsWithoutPk)

    [<Fact>]
    let ``DataMapper should handle complex record with all field types`` () =
        let connection = new SqliteConnection("DataSource=:memory:")
        connection.Open()
        
        let createTableSql = """
            CREATE TABLE complex_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER,
                salary DECIMAL(10,2),
                is_active BOOLEAN NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME,
                score REAL
            )
        """
        
        use cmd = new SqliteCommand(createTableSql, connection)
        cmd.ExecuteNonQuery() |> ignore
        
        let testDate = DateTime(2023, 12, 1, 14, 30, 0)
        let updateDate = DateTime(2024, 1, 15, 10, 0, 0)
        
        let insertSql = """
            INSERT INTO complex_table 
            (id, name, age, salary, is_active, created_at, updated_at, score)
            VALUES 
            (1, 'John Doe', 30, 50000.50, 1, @created, @updated, 95.5),
            (2, 'Jane Smith', NULL, NULL, 0, @created, NULL, NULL)
        """
        
        use insertCmd = new SqliteCommand(insertSql, connection)
        insertCmd.Parameters.AddWithValue("@created", testDate) |> ignore
        insertCmd.Parameters.AddWithValue("@updated", updateDate) |> ignore
        insertCmd.ExecuteNonQuery() |> ignore
        
        let selectCmd = new SqliteCommand("SELECT * FROM complex_table ORDER BY id", connection)
        use reader = selectCmd.ExecuteReader() :> DbDataReader
        
        let results = mapDataReaderToRecords<ComplexRecord> reader
        
        Assert.Equal(2, results.Length)
        
        let first = results.[0]
        Assert.Equal(1, first.Id)
        Assert.Equal("John Doe", first.Name)
        Assert.Equal(Some 30, first.Age)
        Assert.Equal(Some 50000.50m, first.Salary)
        Assert.True(first.IsActive)
        Assert.Equal(testDate, first.CreatedAt)
        Assert.Equal(Some updateDate, first.UpdatedAt)
        Assert.Equal(Some 95.5, first.Score)
        
        let second = results.[1]
        Assert.Equal(2, second.Id)
        Assert.Equal("Jane Smith", second.Name)
        Assert.Equal(None, second.Age)
        Assert.Equal(None, second.Salary)
        Assert.False(second.IsActive)
        Assert.Equal(testDate, second.CreatedAt)
        Assert.Equal(None, second.UpdatedAt)
        Assert.Equal(None, second.Score)

    [<Fact>]
    let ``DataMapper should handle empty strings and special characters`` () =
        let connection = new SqliteConnection("DataSource=:memory:")
        connection.Open()
        
        let createTableSql = """
            CREATE TABLE text_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                description TEXT,
                email TEXT
            )
        """
        
        use cmd = new SqliteCommand(createTableSql, connection)
        cmd.ExecuteNonQuery() |> ignore
        
        // Используем CHAR(10) для символа новой строки
        let insertSql = """
            INSERT INTO text_table (id, name, description, email)
            VALUES 
            (1, '', NULL, 'test@example.com'),
            (2, 'Special "quotes" & <tags>', 'Line1' || CHAR(10) || 'Line2', 'user@test.com')
        """
        
        use insertCmd = new SqliteCommand(insertSql, connection)
        insertCmd.ExecuteNonQuery() |> ignore
        
        let selectCmd = new SqliteCommand("SELECT * FROM text_table ORDER BY id", connection)
        use reader = selectCmd.ExecuteReader() :> DbDataReader
        
        let results = mapDataReaderToRecords<TextRecord> reader
        
        Assert.Equal(2, results.Length)
        Assert.Equal("", results.[0].Name)
        Assert.Equal(None, results.[0].Description)
        Assert.Equal("Special \"quotes\" & <tags>", results.[1].Name)
        let expectedDescription = "Line1" + Environment.NewLine + "Line2"
        Assert.Equal(Some expectedDescription, results.[1].Description)

    [<Fact>]
    let ``DataMapper should handle boolean conversions correctly`` () =
        let connection = new SqliteConnection("DataSource=:memory:")
        connection.Open()
        
        let createTableSql = """
            CREATE TABLE bool_table (
                id INTEGER PRIMARY KEY,
                flag BOOLEAN,
                is_valid BOOLEAN,
                is_active BOOLEAN
            )
        """
        
        use cmd = new SqliteCommand(createTableSql, connection)
        cmd.ExecuteNonQuery() |> ignore
        
        let insertSql = """
            INSERT INTO bool_table (id, flag, is_valid, is_active)
            VALUES 
            (1, 1, 1, 1),
            (2, 0, 0, NULL),
            (3, 1, 0, 1)
        """
        
        use insertCmd = new SqliteCommand(insertSql, connection)
        insertCmd.ExecuteNonQuery() |> ignore
        
        let selectCmd = new SqliteCommand("SELECT * FROM bool_table ORDER BY id", connection)
        use reader = selectCmd.ExecuteReader() :> DbDataReader
        
        let results = mapDataReaderToRecords<BoolRecord> reader
        
        Assert.Equal(3, results.Length)
        Assert.Equal(Some true, results.[0].Flag)
        Assert.True(results.[0].IsValid)
        Assert.Equal(Some true, results.[0].IsActive)
        
        Assert.Equal(Some false, results.[1].Flag)
        Assert.False(results.[1].IsValid)
        Assert.Equal(None, results.[1].IsActive)
        
        Assert.Equal(Some true, results.[2].Flag)
        Assert.False(results.[2].IsValid)
        Assert.Equal(Some true, results.[2].IsActive)