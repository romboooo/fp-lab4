// AUTO-GENERATED FILE - DO NOT EDIT
// Generated at: 2025-12-21 13:06:54
// Generated from database schema

namespace ORM.GeneratedTypes

open System

[<AttributeUsage(AttributeTargets.Property)>]
type PrimaryKeyAttribute() =
    inherit Attribute()

type Orders = {
    [<PrimaryKey>] id: int
    userId: int option
    productId: int option
    quantity: int option
    status: string
    orderDate: System.DateTime option
}

type Products = {
    [<PrimaryKey>] id: int
    name: string
    price: int
    category: string
    inStock: bool option
}

type Users = {
    [<PrimaryKey>] id: int
    username: string
    email: string
    age: int option
    createdAt: System.DateTime option
}
