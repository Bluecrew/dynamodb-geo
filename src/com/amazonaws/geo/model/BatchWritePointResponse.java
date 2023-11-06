package com.amazonaws.geo.model;

import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;

public class BatchWritePointResponse {
	private BatchWriteItemResponse batchWriteItemResponse;
	
	public BatchWritePointResponse(BatchWriteItemResponse batchWriteItemResponse) {
		this.batchWriteItemResponse = batchWriteItemResponse;
	}

	public BatchWriteItemResponse getBatchWriteItemResponse() {
		return batchWriteItemResponse;
	}
}
