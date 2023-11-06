/*
 * Copyright 2010-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.geo.dynamodb.internal;

import java.util.*;

import com.amazonaws.geo.GeoDataManagerConfiguration;
import com.amazonaws.geo.model.*;
import com.amazonaws.geo.model.DeletePointResponse;
import com.amazonaws.geo.s2.internal.S2Manager;
import com.amazonaws.geo.util.GeoJsonMapper;
import software.amazon.awssdk.services.dynamodb.model.*;

public class DynamoDBManager {
	private final GeoDataManagerConfiguration config;

	public DynamoDBManager(GeoDataManagerConfiguration config) {
		this.config = config;
	}

	/**
	 * Query Amazon DynamoDB
	 * 
	 * @param hashKey
	 *            Hash key for the query request.
	 * 
	 * @param range
	 *            The range of geohashs to query.
	 * 
	 * @return The query result.
	 */
	public List<QueryResponse> queryGeohash(long hashKey, GeohashRange range) {
		List<QueryResponse> queryResponses = new ArrayList<>();
		Map<String, AttributeValue> lastEvaluatedKey = null;

		do {
			Map<String, Condition> keyConditions = new HashMap<>();

			Condition hashKeyCondition = Condition.builder()
				.comparisonOperator(ComparisonOperator.EQ)
				.attributeValueList(AttributeValue.builder().n(String.valueOf(hashKey)).build()).build();
			keyConditions.put(config.getHashKeyAttributeName(), hashKeyCondition);

			AttributeValue minRange = AttributeValue.builder().n(Long.toString(range.getRangeMin())).build();
			AttributeValue maxRange = AttributeValue.builder().n(Long.toString(range.getRangeMax())).build();

			Condition geohashCondition = Condition.builder().comparisonOperator(ComparisonOperator.BETWEEN)
				.attributeValueList(minRange, maxRange).build();
			keyConditions.put(config.getGeohashAttributeName(), geohashCondition);

			QueryRequest queryRequest = QueryRequest.builder()
				.tableName(config.getTableName())
				.keyConditions(keyConditions)
				.indexName(config.getGeohashIndexName())
				.consistentRead(false)
				.returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
				.exclusiveStartKey(lastEvaluatedKey).build();

			QueryResponse queryResponse = config.getDynamoDBClient().query(queryRequest);
			queryResponses.add(queryResponse);

			lastEvaluatedKey = queryResponse.hasLastEvaluatedKey() ? queryResponse.lastEvaluatedKey() : null;

		} while (lastEvaluatedKey != null);

		return queryResponses;
	}

	public GetPointResponse getPoint(GetPointRequest getPointRequest) {
		long geohash = S2Manager.generateGeohash(getPointRequest.getGeoPoint());
		long hashKey = S2Manager.generateHashKey(geohash, config.getHashKeyLength());
		AttributeValue hashKeyValue = AttributeValue.builder().n(String.valueOf(hashKey)).build();
		GetItemRequest getItemRequest = GetItemRequest.builder()
			.tableName(config.getTableName())
			.key(Map.of(config.getHashKeyAttributeName(), hashKeyValue,
				config.getRangeKeyAttributeName(), getPointRequest.getRangeKeyValue())).build();
		GetItemResponse getItemResponse = config.getDynamoDBClient().getItem(getItemRequest);

        return new GetPointResponse(getItemResponse);
	}

	public PutPointResponse putPoint(PutPointRequest putPointRequest) {
		long geohash = S2Manager.generateGeohash(putPointRequest.getGeoPoint());
		long hashKey = S2Manager.generateHashKey(geohash, config.getHashKeyLength());
		String geoJson = GeoJsonMapper.stringFromGeoObject(putPointRequest.getGeoPoint());

		AttributeValue hashKeyValue = AttributeValue.builder().n(String.valueOf(hashKey)).build();
		AttributeValue geohashValue = AttributeValue.builder().n(Long.toString(geohash)).build();
		AttributeValue geoJsonValue = AttributeValue.builder().s(geoJson).build();

		PutItemRequest putItemRequest = PutItemRequest.builder()
			.tableName(config.getTableName())
			.item(Map.of(config.getHashKeyAttributeName(), hashKeyValue,
		config.getRangeKeyAttributeName(), putPointRequest.getRangeKeyValue(),
		config.getGeohashAttributeName(), geohashValue,
		config.getGeoJsonAttributeName(), geoJsonValue))
			.build();
		PutItemResponse putItemResponse = config.getDynamoDBClient().putItem(putItemRequest);

        return new PutPointResponse(putItemResponse);
	}
	
	public BatchWritePointResponse batchWritePoints(List<PutPointRequest> putPointRequests) {
		List<WriteRequest> writeRequests = new ArrayList<>();
		for (PutPointRequest putPointRequest : putPointRequests) {
			long geohash = S2Manager.generateGeohash(putPointRequest.getGeoPoint());
			long hashKey = S2Manager.generateHashKey(geohash, config.getHashKeyLength());
			String geoJson = GeoJsonMapper.stringFromGeoObject(putPointRequest.getGeoPoint());

			AttributeValue hashKeyValue = AttributeValue.builder().n(String.valueOf(hashKey)).build();
			AttributeValue geohashValue = AttributeValue.builder().n(Long.toString(geohash)).build();
			AttributeValue geoJsonValue = AttributeValue.builder().s(geoJson).build();

			PutRequest putRequest = PutRequest.builder()
			.item(Map.of(config.getHashKeyAttributeName(), hashKeyValue,
			config.getRangeKeyAttributeName(), putPointRequest.getRangeKeyValue(),
			config.getGeohashAttributeName(), geohashValue,
			config.getGeoJsonAttributeName(), geoJsonValue))
			.build();
			WriteRequest writeRequest = WriteRequest.builder().putRequest(putRequest).build();
			writeRequests.add(writeRequest);
		}
		Map<String, List<WriteRequest>> requestItems = new HashMap<>();
		requestItems.put(config.getTableName(), writeRequests);
		BatchWriteItemRequest batchItemRequest = BatchWriteItemRequest.builder().requestItems(requestItems).build();
		BatchWriteItemResponse batchWriteItemResponse = config.getDynamoDBClient().batchWriteItem(batchItemRequest);
        return new BatchWritePointResponse(batchWriteItemResponse);
	}

	public UpdatePointResponse updatePoint(UpdatePointRequest updatePointRequest, Map<String, AttributeValueUpdate> updates) {
		long geohash = S2Manager.generateGeohash(updatePointRequest.getGeoPoint());
		long hashKey = S2Manager.generateHashKey(geohash, config.getHashKeyLength());

		Map<String, AttributeValueUpdate> updatedItems = new HashMap<>();

		// Geohash and geoJson cannot be updated.
		for (String updateKey : updates.keySet()) {
			if (!config.getGeohashAttributeName().equals(updateKey) && !config.getGeohashAttributeName().equals(updateKey)) {
				updatedItems.put(updateKey, updates.get(updateKey));
			}
		}

		AttributeValue hashKeyValue = AttributeValue.builder().n(String.valueOf(hashKey)).build();
		UpdateItemRequest updateItemRequest = UpdateItemRequest.builder().tableName(config.getTableName())
		.key(Map.of(config.getHashKeyAttributeName(), hashKeyValue,
		config.getRangeKeyAttributeName(), updatePointRequest.getRangeKeyValue()))
			.attributeUpdates(updatedItems)
			.build();


		UpdateItemResponse updateItemResponse = config.getDynamoDBClient().updateItem(updateItemRequest);

        return new UpdatePointResponse(updateItemResponse);
	}

	public DeletePointResponse deletePoint(DeletePointRequest deletePointRequest) {
		long geohash = S2Manager.generateGeohash(deletePointRequest.getGeoPoint());
		long hashKey = S2Manager.generateHashKey(geohash, config.getHashKeyLength());

		AttributeValue hashKeyValue = AttributeValue.builder().n(String.valueOf(hashKey)).build();

		DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder().tableName(config.getTableName())
			.key(Map.of(config.getHashKeyAttributeName(), hashKeyValue,
		config.getRangeKeyAttributeName(), deletePointRequest.getRangeKeyValue()))
			.build();

		DeleteItemResponse deleteItemResponse = config.getDynamoDBClient().deleteItem(deleteItemRequest);

        return new DeletePointResponse(deleteItemResponse);
	}
}
