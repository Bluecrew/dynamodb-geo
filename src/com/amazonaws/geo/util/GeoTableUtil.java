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

package com.amazonaws.geo.util;

import com.amazonaws.geo.GeoDataManagerConfiguration;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Utility class.
 * */
public class GeoTableUtil {

	/**
	 * <p>
	 * Construct a create table request object based on GeoDataManagerConfiguration. The users can update any aspect of
	 * the request and call it.
	 * </p>
	 * Example:
	 * 
	 * <pre>
	 * AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(new ClasspathPropertiesFileCredentialsProvider());
	 * Region usWest2 = Region.getRegion(Regions.US_WEST_2);
	 * ddb.setRegion(usWest2);
	 * 
	 * CreateTableRequest createTableRequest = GeoTableUtil.getCreateTableRequest(config);
	 * CreateTableResult createTableResult = ddb.createTable(createTableRequest);
	 * </pre>
	 * 
	 * @return Generated create table request.
	 */
	public static CreateTableRequest getCreateTableRequest(GeoDataManagerConfiguration config) {
		CreateTableRequest createTableRequest = CreateTableRequest.builder()
				.tableName(config.getTableName())
				.provisionedThroughput(
						ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(5L).build())
				.keySchema(
						KeySchemaElement.builder().keyType(KeyType.HASH).attributeName(
								config.getHashKeyAttributeName()).build(),
						KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName(
								config.getRangeKeyAttributeName()).build())
				.attributeDefinitions(
						AttributeDefinition.builder().attributeType(ScalarAttributeType.N).attributeName(
								config.getHashKeyAttributeName()).build(),
						AttributeDefinition.builder().attributeType(ScalarAttributeType.S).attributeName(
								config.getRangeKeyAttributeName()).build(),
						AttributeDefinition.builder().attributeType(ScalarAttributeType.N).attributeName(
								config.getGeohashAttributeName()).build())
				.localSecondaryIndexes(
						LocalSecondaryIndex.builder()
								.indexName(config.getGeohashIndexName())
								.keySchema(
										KeySchemaElement.builder().keyType(KeyType.HASH).attributeName(
												config.getHashKeyAttributeName()).build(),
										KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName(
												config.getGeohashAttributeName()).build())
								.projection(Projection.builder().projectionType(ProjectionType.ALL).build()).build()).build();

		return createTableRequest;
	}
}
