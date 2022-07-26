package com.mhedu.athena

import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.athena.model.{QueryExecutionContext,
  StartQueryExecutionRequest, StartQueryExecutionResponse}
import software.amazon.awssdk.services.athena.AthenaClient
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest
import software.amazon.awssdk.services.athena.model.QueryExecutionState
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest

/**
 *  Athena client for executing queries against the specified database using the given
 *  workgroup, and output bucket.
 */
class AthenaQueryClient(database: String, workGroup: String) {
  val instanceProfile = InstanceProfileCredentialsProvider.create()
  val athenaClient = AthenaClient.builder().region(Region.US_EAST_1)
    .credentialsProvider(instanceProfile)
    .build();

  /**
   * Submit Athena query
   *
   * @param query
   *
   * @return queryExecutionResponse
   */
  def submitQuery(query: String): String =
  {
    val queryExecutionContext = QueryExecutionContext.builder().database(database).build();
    val startQueryExecutionRequest = StartQueryExecutionRequest.builder()
      .queryString(query)
      .queryExecutionContext(queryExecutionContext)
      .workGroup(workGroup)
      .build();

    athenaClient.startQueryExecution(startQueryExecutionRequest).queryExecutionId()
  }

  /**
   * Submit a drop table statement
   *
   * @param tableName
   *
   * @return StartQueryExecutionResponse
   */
  def dropTable(tableName: String): String = {
    submitQuery(s"drop table $tableName")
  }

  def getQueryState(getQueryExecutionRequest: GetQueryExecutionRequest)= {
    val getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest)
    val queryState = getQueryExecutionResponse.queryExecution.status.state.toString

    import software.amazon.awssdk.services.athena.model.QueryExecutionState
    if (queryState.equals(QueryExecutionState.FAILED.toString)) {
      throw new RuntimeException("The Amazon Athena query failed to run with error message: " + getQueryExecutionResponse.queryExecution.status.stateChangeReason)
    }
    else if (queryState.equals(QueryExecutionState.CANCELLED.toString)) {
      throw new RuntimeException("The Amazon Athena query was cancelled.")
    }

    queryState
  }

  /**
   * Wait for an Amazon Athena query to complete, fail or to be cancelled
   */
  @throws[InterruptedException]
  def waitForQueryToComplete(queryExecutionId: String): Unit = {
    val getQueryExecutionRequest = GetQueryExecutionRequest.builder.queryExecutionId(queryExecutionId).build

    while (getQueryState(getQueryExecutionRequest) != QueryExecutionState.SUCCEEDED.toString) {
        Thread.sleep(10000)
    }
  }


  /**
   * Test to see if the query result set is non-empty.
   *
   * @param queryExecutionId
   *
   * @return  True if non-empty, false oterwise
   */
  def checkQueryResult(queryExecutionId: String): Boolean = {
    val getQueryResultsRequest = GetQueryResultsRequest.builder.queryExecutionId(queryExecutionId).build

    athenaClient.getQueryResultsPaginator(getQueryResultsRequest).iterator().hasNext
  }


}


