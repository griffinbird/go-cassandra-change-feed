---
page_type: sample
languages:
- go
products:
- azure
description: "Azure Cosmos DB is a globally distributed multi-model database. One of the supported APIs is the Cassandra API"
urlFragment: azure-cosmos-db-cassandra-go-changefeed
---

# Developing a Go app with Cassandra API using Azure Cosmos DB (`gocql` Driver) using Cosmos DB Change Feed

[Azure Cosmos DB]((https://docs.microsoft.com/azure/cosmos-db/introduction)) is a globally distributed multi-model database. One of the supported APIs is the [Cassandra API](https://docs.microsoft.com/azure/cosmos-db/cassandra-introduction). 

The code included in this sample is intended to get you quickly started with a Go application that connects to Azure Cosmos DB with the Cassandra API. It walks you through creation of keyspace, table, inserting and querying the data using the Cosmos DB Change Feed.

## Prerequisites

Before you can run this sample, you must have the following prerequisites:

- An Azure account with an active subscription. [Create one for free](https://azure.microsoft.com/free/?WT.mc_id=cassandrago-github-abhishgu). Or [try Azure Cosmos DB for free](https://azure.microsoft.com/try/cosmosdb/?WT.mc_id=cassandrago-github-abhishgu) without an Azure subscription.
- [Go](https://golang.org/) installed on your computer, and a working knowledge of Go.
- [Git](https://git-scm.com/downloads).

## Running this sample

1. Clone this repository using `git clone https://github.com/griffinbird/go-cassandra-change-feed`

2. Change directories to the repo using `cd go-cassandra-change-feed`

3. Set environment variables. Either in the shell or via .env

```shell
export COSMOSDB_CASSANDRA_CONTACT_POINT=<Contact Point for Azure Cosmos DB Cassandra API>
export COSMOSDB_CASSANDRA_PORT=<Port for Azure Cosmos DB Cassandra API>
export COSMOSDB_CASSANDRA_USER=<Username for Azure Cosmos DB Cassandra API>
export COSMOSDB_CASSANDRA_PASSWORD=<Password for Azure Cosmos DB Cassandra API>
```

4. Run the application

```shell
go mod tidy
go run main.go
```

## More information

- [Azure Cosmos DB](https://docs.microsoft.com/azure/cosmos-db/introduction?WT.mc_id=cassandrago-github-abhishgu)
- [Azure Cosmos DB for Cassandra API](https://docs.microsoft.com/azure/cosmos-db/cassandra-introduction?WT.mc_id=cassandrago-github-abhishgu)
- [gocql - Cassandra Go driver](https://github.com/gocql/gocql)
- [gocql reference](https://godoc.org/github.com/gocql/gocql)