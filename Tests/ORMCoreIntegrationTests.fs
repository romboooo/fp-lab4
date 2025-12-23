namespace ORM.Tests

open System
open Xunit
open ORM.Database
open ORM.ORMCore
open ORM.GeneratedTypes

module ORMCoreIntegrationTests =
    
    [<Fact>]
    let ``Full CRUD cycle should work with real database`` () =
        use db = new DatabaseConnection()
        let usersTable = db.Table<Users>("users")
        
        let testId = Guid.NewGuid().ToString("N").Substring(0, 8)
        let testUsername = $"integration_test_{testId}"
        let testEmail = $"integration_{testId}@example.com"
        
        try
            let insertResult = 
                CRUD.insert usersTable [
                    "username", box testUsername
                    "email", box testEmail
                    "age", box 35
                ]
            
            match insertResult with
            | Error err -> 
                Assert.True(false, $"Insert failed: {err}")
            | Ok newId ->
                match CRUD.findById usersTable newId with
                | Error err ->
                    Assert.True(false, $"Find by ID failed: {err}")
                | Ok None -> 
                    Assert.True(false, "Record not found after insert")
                | Ok (Some user) ->
                    Assert.Equal(testUsername, user.username)
                    Assert.Equal(testEmail, user.email)
                    Assert.Equal(Some 35, user.age)
                    
                    let updateResult = 
                        CRUD.update usersTable newId [
                            "age", box 36
                            "email", box $"updated_{testId}@example.com"
                        ]
                    
                    match updateResult with
                    | Error err -> 
                        Assert.True(false, $"Update failed: {err}")
                    | Ok rowsUpdated ->
                        Assert.Equal(1, rowsUpdated)
                        
                        match CRUD.findById usersTable newId with
                        | Ok (Some updatedUser) ->
                            Assert.Equal(Some 36, updatedUser.age)
                            Assert.Equal($"updated_{testId}@example.com", updatedUser.email)
                            
                            let deleteResult = CRUD.delete usersTable newId
                            match deleteResult with
                            | Error err -> 
                                Assert.True(false, $"Delete failed: {err}")
                            | Ok rowsDeleted ->
                                Assert.Equal(1, rowsDeleted)
                                
                                match CRUD.findById usersTable newId with
                                | Ok None -> 
                                    Assert.True(true) 
                                | Ok (Some _) -> 
                                    Assert.True(false, "Record still exists after delete")
                                | Error err -> 
                                    Assert.True(false, $"Find after delete failed: {err}")
                        | Ok None -> 
                            Assert.True(false, "Record not found after update")
                        | Error err -> 
                            Assert.True(false, $"Find after update failed: {err}")
        finally
            let cleanupResult = 
                CRUD.deleteWhere usersTable (
                    ORM.QueryBuilder.Condition.like "username" "integration_test_%"
                )
            match cleanupResult with
            | Ok count -> 
                if count > 0 then 
                    printfn $"Cleaned up {count} test records"
            | Error _ -> ()

module Program = 
    [<EntryPoint>]
    let main _ =
        printfn "ORM Tests - Use 'dotnet test' to run tests"
        0