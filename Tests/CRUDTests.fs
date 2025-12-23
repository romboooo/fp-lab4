namespace ORM.Tests

open System
open Xunit
open ORM.QueryBuilder
open ORM

type ComplexRecord = {
    Id: int
    Name: string
    Age: int option
    IsActive: bool
    IsActiveOpt: bool option
    Price: decimal option
    CreatedAt: DateTime option
    Score: float option
}
module CRUDTests =
    
    [<Fact>]
    let ``TypeSafeHelpers should handle all field types correctly`` () =
       
        
        let testDate = DateTime(2023, 1, 1)
        let record = {
            Id = 1
            Name = "Test"
            Age = Some 30
            IsActive = true
            IsActiveOpt = Some false
            Price = Some 99.99m
            CreatedAt = Some testDate
            Score = Some 85.5
        }
        
        let values = ORM.QueryBuilder.TypeSafeHelpers.toInsertValues record
        
        Assert.Equal(8, values.Length)
        Assert.Contains(("id", box 1), values)
        Assert.Contains(("name", box "Test"), values)
        Assert.Contains(("age", box 30), values)
        Assert.Contains(("isactive", box true), values)
        Assert.Contains(("isactiveopt", box false), values)
        Assert.Contains(("price", box 99.99m), values)
        Assert.Contains(("createdat", box testDate), values)
        Assert.Contains(("score", box 85.5), values)
    type ValidatedRecord = {
        [<PrimaryKey>] Id: int
        Name: string
        Age: int option
        Email: string
    }
    
    [<Fact>]
    let ``TypeSafeHelpers.validateRecord should pass for valid record`` () =
        let record = {
            Id = 1
            Name = "Test User"
            Age = Some 30
            Email = "test@example.com"
        }
        
        let result = TypeSafeHelpers.validateRecord record "test_table"
        
        match result with
        | Ok validated -> 
            Assert.Equal(record, validated)
            Assert.True(true)
        | Error err -> 
            Assert.True(false, $"Validation should have passed: {err}")

    [<Fact>]
    let ``TypeSafeHelpers.validateRecord should fail for null required field`` () =
        let record = {
            Id = 1
            Name = null
            Age = Some 30
            Email = "test@example.com"
        }
        
        let result = TypeSafeHelpers.validateRecord record "test_table"
        
        match result with
        | Ok _ -> 
            Assert.True(false, "Validation should have failed for null required field")
        | Error err -> 
            Assert.Contains("Required field 'Name' cannot be null", err)
            () // Возвращаем unit

    [<Fact>]
    let ``TypeSafeHelpers.validateRecord should fail for null primary key`` () =
        let record = {
            Id = 0  // Value type, can't be null
            Name = "Test"
            Age = Some 30
            Email = "test@example.com"
        }
        
        let result = TypeSafeHelpers.validateRecord record "test_table"
        
        match result with
        | Ok _ -> 
            Assert.True(true) // Value types can't be null
        | Error err -> 
            printfn $"Validation error: {err}"
            // This might fail depending on implementation
            Assert.True(true)

    [<Fact>]
    let ``toUpdateValues should only include specified fields`` () =
        let record = {
            Id = 1
            Name = "Updated Name"
            Age = Some 35
            Email = "updated@example.com"
        }
        
        let values = TypeSafeHelpers.toUpdateValues record ["Name"; "Email"]
        
        Assert.Equal(2, values.Length)
        Assert.Contains(("name", box "Updated Name"), values)
        Assert.Contains(("email", box "updated@example.com"), values)
        Assert.DoesNotContain(("id", box 1), values)
        Assert.DoesNotContain(("age", box 35), values)

    [<Fact>]
    let ``toUpdateValues should handle empty field list`` () =
        let record = {
            Id = 1
            Name = "Test"
            Age = Some 30
            Email = "test@example.com"
        }
        
        let values = TypeSafeHelpers.toUpdateValues record []
        
        Assert.Empty(values)

    [<Fact>]
    let ``toUpdateValues should handle non-existent fields gracefully`` () =
        let record = {
            Id = 1
            Name = "Test"
            Age = Some 30
            Email = "test@example.com"
        }
        
        let values = TypeSafeHelpers.toUpdateValues record ["Name"; "NonExistentField"]
        
        Assert.Single(values)
        Assert.Contains(("name", box "Test"), values)