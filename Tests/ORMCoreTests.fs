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
        let ageValue = values |> List.tryFind (fun (k, _) -> k = "age") |> Option.map snd
        match ageValue with
        | Some v -> Assert.Equal(30, v :?> int)
        | None -> Assert.True(false, "Age not found")