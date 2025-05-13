package com.binvij

import scala.concurrent.Await
import scala.concurrent.duration.Duration


object MainApp extends App {
  println("staring program...")

  // Create a Service Principal in Azure, provide EventHub Owner or Data Sender Permissions
  // https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-dotnet-standard-getstarted-send?tabs=passwordless%2Croles-azure-portal
  // and set environment variables for Service Principal
  System.setProperty("AZURE_CLIENT_ID", "{SET SPN APP ID}") // TODO
  System.setProperty("AZURE_CLIENT_SECRET", "{SET APP SECRET}") // TODO
  System.setProperty("AZURE_TENANT_ID", "{SET TENANT ID}") // TODO

  private val processFuture = EventHubProcessor("Hate_Crimes.csv")
    .authenticate()
    .readCSV()
    .processBatch()

  // since this is a CLI, we need to wait the main thread for the others to finish
  Await.result(processFuture, Duration.Inf)

  println("program finished!")

}
