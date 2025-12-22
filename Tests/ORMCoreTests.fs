// tests/ORMCoreTests.fs
namespace ORM.Tests

open Xunit
open ORM.QueryBuilder

module ORMCoreTests =

    type TestRecord = { 
        Id: int
        Name: string 
        Age: int option 
    }

    [<Fact>]
    let ``TypeSafeHelpers should convert record`` () =
        let record = { 
            Id = 1
            Name = "Test" 
            Age = Some 30 
        }
        
        let values = TypeSafeHelpers.toInsertValues record
        
        Assert.Equal(3, values.Length)
        Assert.Contains(("id", box 1), values)
        Assert.Contains(("name", box "Test"), values)