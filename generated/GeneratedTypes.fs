namespace ORM.GeneratedTypes

// AUTO-GENERATED FILE - DO NOT EDIT
// Generated at: 2025-12-23 18:45:19
// Generated from database schema

open System
open ORM

type Orders =
    { [<PrimaryKey>]
      id: int
      userId: int option
      productId: int option
      quantity: int option
      status: string option
      orderDate: System.DateTime option }

type Products =
    { [<PrimaryKey>]
      id: int
      name: string
      price: int
      category: string option
      inStock: bool option }

type Users =
    { [<PrimaryKey>]
      id: int
      username: string
      email: string
      age: int option
      createdAt: System.DateTime option }
