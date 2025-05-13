package com.binvij

import com.azure.identity.{AzureAuthorityHosts, DefaultAzureCredential, DefaultAzureCredentialBuilder}
import com.azure.messaging.eventhubs.{EventData, EventHubClientBuilder}
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class EventHubProcessor(fileName: String) {
  private var credentials: Option[DefaultAzureCredential] = None
  private var jsonResp: Array[String] = new Array[String](0)
  private val eventHubsNamespace = "{your-eventhub-namespace}.servicebus.windows.net" // TODO
  private val eventHubName = "{your-topic/eventhub-name}" // TODO

  def authenticate(): EventHubProcessor = {
    try {
      credentials = Some(new DefaultAzureCredentialBuilder()
        .authorityHost(AzureAuthorityHosts.AZURE_PUBLIC_CLOUD)
        .build()
      )
    }catch {
      case ex: Exception => {credentials = None}
    }
    this
  }

  def readCSV(): EventHubProcessor = {
    val asyncResult = async {
      val csvReader = SparkCsvReader(fileName)
      jsonResp = await (csvReader.fromCsvToJson())
    }
    Await.result(asyncResult, Duration.Inf)
    this
  }

  def processBatch(): Future[Unit] = {
    Future {
      val cred = credentials match {
        case Some(x) => x
      }
      val producer = new EventHubClientBuilder().fullyQualifiedNamespace(eventHubsNamespace)
        .eventHubName(eventHubName)
        .credential(cred)
        .buildProducerClient()
      // create a batch
      var eventDataBatch = producer.createBatch()
      // iterate through each row and fill the batch
      jsonResp.foreach(row => {
        val eventData = new EventData(row)
        if (!eventDataBatch.tryAdd(eventData)) {
          // if we are not able to add the batch then the batch is full
          // try to send the batch and create a new one
          println("Batch is full, sending the batch..")
          producer.send(eventDataBatch)
          println("sent..")
          // create a new batch
          eventDataBatch = producer.createBatch()
          // try to add the batch now that couldn't fit before
          if (!eventDataBatch.tryAdd(eventData)) {
            println(s"failed to send event data. Event too large for empty batch: ${eventDataBatch.getMaxSizeInBytes}")
          } else
            println("added the event data successfully.")
        }
      })
      // if there is remaining batch or the batch was never sent, then send it
      if (eventDataBatch.getCount() > 0) {
        producer.send(eventDataBatch)
        println("Sent all the event batches successfully")
      }
      producer.close()
    }
  }
}

object EventHubProcessor {
  def apply(fileName: String) = {
    new EventHubProcessor(fileName)
  }
}
