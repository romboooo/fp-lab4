namespace ORM.Tests

open System
open System.Data.Common
open Xunit
open ORM
open ORM.DataMapper
open Microsoft.Data.Sqlite

module DataMapperTests =

    type TestRecord = {
        [<PrimaryKey>] Id: int  // Добавляем атрибут PrimaryKey
        Name: string
        Age: int option
        CreatedAt: DateTime option
        Price: decimal option
        Score: float option
        IsActive: bool
        IsActiveOpt: bool option
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
        
        // Тестируем включение первичного ключа
        let paramsWithPk = RecordConverter.recordToParameterList record true
        Assert.Equal(8, paramsWithPk.Length)
        Assert.Contains(("id", box 1), paramsWithPk)
        Assert.Contains(("name", box "Test"), paramsWithPk)
        Assert.Contains(("age", box 30), paramsWithPk)
        
        // Тестируем исключение первичного ключа
        let paramsWithoutPk = RecordConverter.recordToParameterList record false
        Assert.Equal(7, paramsWithoutPk.Length)
        Assert.DoesNotContain(("id", box 1), paramsWithoutPk)
        Assert.Contains(("name", box "Test"), paramsWithoutPk)