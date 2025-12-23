namespace ORM.Tests

open System
open Xunit
open ORM.QueryBuilder

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