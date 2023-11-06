/*
 * Copyright 2010-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 * http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.geo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import software.amazon.awssdk.core.exception.SdkException;
import com.amazonaws.geo.dynamodb.internal.DynamoDBManager;
import com.amazonaws.geo.dynamodb.internal.DynamoDBUtil;
import com.amazonaws.geo.model.*;
import com.amazonaws.geo.model.DeletePointResponse;
import com.amazonaws.geo.s2.internal.S2Manager;
import com.amazonaws.geo.s2.internal.S2Util;
import com.amazonaws.geo.util.GeoJsonMapper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

/**
 * <p>
 * Manager to hangle geo spatial data in Amazon DynamoDB tables. All service calls made using this client are blocking,
 * and will not return until the service call completes.
 * </p>
 * <p>
 * This class is designed to be thread safe; however, once constructed GeoDataManagerConfiguration should not be
 * modified. Modifying GeoDataManagerConfiguration may cause unspecified behaviors.
 * </p>
 * */
public class GeoDataManager {
	private final GeoDataManagerConfiguration config;
	private final DynamoDBManager dynamoDBManager;

	/**
	 * <p>
	 * Construct and configure GeoDataManager using GeoDataManagerConfiguration.
	 * </p>
	 * <b>Sample usage:</b>
	 * 
	 * <pre>
	 * AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(new ClasspathPropertiesFileCredentialsProvider());
	 * Region usWest2 = Region.getRegion(Regions.US_WEST_2);
	 * ddb.setRegion(usWest2);
	 * 
	 * ClientConfiguration clientConfiguration = new ClientConfiguration().withMaxErrorRetry(5);
	 * ddb.setConfiguration(clientConfiguration);
	 * 
	 * GeoDataManagerConfiguration config = new GeoDataManagerConfiguration(ddb, &quot;geo-table&quot;);
	 * GeoDataManager geoDataManager = new GeoDataManager(config);
	 * </pre>
	 * 
	 * @param config
	 *            Container for the configuration parameters for GeoDataManager.
	 */
	public GeoDataManager(GeoDataManagerConfiguration config) {
		this.config = config;
		dynamoDBManager = new DynamoDBManager(this.config);
	}

	/**
	 * <p>
	 * Return GeoDataManagerConfiguration. The returned GeoDataManagerConfiguration should not be modified.
	 * </p>
	 * 
	 * @return
	 *         GeoDataManagerConfiguration that is used to configure this GeoDataManager.
	 */
	public GeoDataManagerConfiguration getGeoDataManagerConfiguration() {
		return config;
	}

	/**
	 * <p>
	 * Put a point into the Amazon DynamoDB table. Once put, you cannot update attributes specified in
	 * GeoDataManagerConfiguration: hash key, range key, geohash and geoJson. If you want to update these columns, you
	 * need to insert a new record and delete the old record.
	 * </p>
	 * <b>Sample usage:</b>
	 * 
	 * <pre>
	 * GeoPoint geoPoint = new GeoPoint(47.5, -122.3);
	 * AttributeValue rangeKeyValue = new AttributeValue().withS(&quot;a6feb446-c7f2-4b48-9b3a-0f87744a5047&quot;);
	 * AttributeValue titleValue = new AttributeValue().withS(&quot;Original title&quot;);
	 * 
	 * PutPointRequest putPointRequest = new PutPointRequest(geoPoint, rangeKeyValue);
	 * putPointRequest.getPutItemRequest().getItem().put(&quot;title&quot;, titleValue);
	 * 
	 * PutPointResponse putPointResponse = geoDataManager.putPoint(putPointRequest);
	 * </pre>
	 * 
	 * @param putPointRequest
	 *            Container for the necessary parameters to execute put point request.
	 * 
	 * @return Response of put point request.
	 */
	public PutPointResponse putPoint(PutPointRequest putPointRequest) {
		return dynamoDBManager.putPoint(putPointRequest);
	}
	
	/**
	 * <p>
	 * Put a list of points into the Amazon DynamoDB table. Once put, you cannot update attributes specified in
	 * GeoDataManagerConfiguration: hash key, range key, geohash and geoJson. If you want to update these columns, you
	 * need to insert a new record and delete the old record.
	 * </p>
	 * <b>Sample usage:</b>
	 * 
	 * <pre>
	 * GeoPoint geoPoint = new GeoPoint(47.5, -122.3);
	 * AttributeValue rangeKeyValue = new AttributeValue().withS(&quot;a6feb446-c7f2-4b48-9b3a-0f87744a5047&quot;);
	 * AttributeValue titleValue = new AttributeValue().withS(&quot;Original title&quot;);
	 * 
	 * PutPointRequest putPointRequest = new PutPointRequest(geoPoint, rangeKeyValue);
	 * putPointRequest.getPutItemRequest().getItem().put(&quot;title&quot;, titleValue);
	 * List<PutPointRequest> putPointRequests = new ArrayList<PutPointRequest>();
	 * putPointRequests.add(putPointRequest);
	 * BatchWritePointResponse batchWritePointResponse = geoDataManager.batchWritePoints(putPointRequests);
	 * </pre>
	 * 
	 * @param putPointRequests
	 *            Container for the necessary parameters to execute put point request.
	 * 
	 * @return Response of batch put point request.
	 */	
	public BatchWritePointResponse batchWritePoints(List<PutPointRequest> putPointRequests) {
		return dynamoDBManager.batchWritePoints(putPointRequests);
	}

	/**
	 * <p>
	 * Get a point from the Amazon DynamoDB table.
	 * </p>
	 * <b>Sample usage:</b>
	 * 
	 * <pre>
	 * GeoPoint geoPoint = new GeoPoint(47.5, -122.3);
	 * AttributeValue rangeKeyValue = new AttributeValue().withS(&quot;a6feb446-c7f2-4b48-9b3a-0f87744a5047&quot;);
	 * 
	 * GetPointRequest getPointRequest = new GetPointRequest(geoPoint, rangeKeyValue);
	 * GetPointResponse getPointResponse = geoIndexManager.getPoint(getPointRequest);
	 * 
	 * System.out.println(&quot;item: &quot; + getPointResponse.getGetItemResponse().getItem());
	 * </pre>
	 * 
	 * @param getPointRequest
	 *            Container for the necessary parameters to execute get point request.
	 * 
	 * @return Response of get point request.
	 * */
	public GetPointResponse getPoint(GetPointRequest getPointRequest) {
		return dynamoDBManager.getPoint(getPointRequest);
	}

	/**
	 * <p>
	 * Query a rectangular area constructed by two points and return all points within the area. Two points need to
	 * construct a rectangle from minimum and maximum latitudes and longitudes. If minPoint.getLongitude() >
	 * maxPoint.getLongitude(), the rectangle spans the 180 degree longitude line.
	 * </p>
	 * <b>Sample usage:</b>
	 * 
	 * <pre>
	 * GeoPoint minPoint = new GeoPoint(45.5, -124.3);
	 * GeoPoint maxPoint = new GeoPoint(49.5, -120.3);
	 * 
	 * QueryRectangleRequest queryRectangleRequest = new QueryRectangleRequest(minPoint, maxPoint);
	 * QueryRectangleResponse queryRectangleResponse = geoIndexManager.queryRectangle(queryRectangleRequest);
	 * 
	 * for (Map&lt;String, AttributeValue&gt; item : queryRectangleResponse.getItem()) {
	 * 	System.out.println(&quot;item: &quot; + item);
	 * }
	 * </pre>
	 * 
	 * @param queryRectangleRequest
	 *            Container for the necessary parameters to execute rectangle query request.
	 * 
	 * @return Response of rectangle query request.
	 */
	public QueryRectangleResponse queryRectangle(QueryRectangleRequest queryRectangleRequest) {
		S2LatLngRect latLngRect = S2Util.getBoundingLatLngRect(queryRectangleRequest);

		S2CellUnion cellUnion = S2Manager.findCellIds(latLngRect);

		List<GeohashRange> ranges = mergeCells(cellUnion);
		cellUnion = null;

		return new QueryRectangleResponse(dispatchQueries(ranges, queryRectangleRequest));
	}

	/**
	 * <p>
	 * Query a circular area constructed by a center point and its radius.
	 * </p>
	 * <b>Sample usage:</b>
	 * 
	 * <pre>
	 * GeoPoint centerPoint = new GeoPoint(47.5, -122.3);
	 * 
	 * QueryRadiusRequest queryRadiusRequest = new QueryRadiusRequest(centerPoint, 100);
	 * QueryRadiusResponse queryRadiusResponse = geoIndexManager.queryRadius(queryRadiusRequest);
	 * 
	 * for (Map&lt;String, AttributeValue&gt; item : queryRadiusResponse.getItem()) {
	 * 	System.out.println(&quot;item: &quot; + item);
	 * }
	 * </pre>
	 * 
	 * @param queryRadiusRequest
	 *            Container for the necessary parameters to execute radius query request.
	 * 
	 * @return Response of radius query request.
	 * */
	public QueryRadiusResponse queryRadius(QueryRadiusRequest queryRadiusRequest) {
		S2LatLngRect latLngRect = S2Util.getBoundingLatLngRect(queryRadiusRequest);

		S2CellUnion cellUnion = S2Manager.findCellIds(latLngRect);

		List<GeohashRange> ranges = mergeCells(cellUnion);
		cellUnion = null;

		return new QueryRadiusResponse(dispatchQueries(ranges, queryRadiusRequest));
	}

	/**
	 * <p>
	 * Update a point data in Amazon DynamoDB table. You cannot update attributes specified in
	 * GeoDataManagerConfiguration: hash key, range key, geohash and geoJson. If you want to update these columns, you
	 * need to insert a new record and delete the old record.
	 * </p>
	 * <b>Sample usage:</b>
	 * 
	 * <pre>
	 * GeoPoint geoPoint = new GeoPoint(47.5, -122.3);
	 * 
	 * String rangeKey = &quot;a6feb446-c7f2-4b48-9b3a-0f87744a5047&quot;;
	 * AttributeValue rangeKeyValue = new AttributeValue().withS(rangeKey);
	 * 
	 * UpdatePointRequest updatePointRequest = new UpdatePointRequest(geoPoint, rangeKeyValue);
	 * 
	 * AttributeValue titleValue = new AttributeValue().withS(&quot;Updated title.&quot;);
	 * AttributeValueUpdate titleValueUpdate = new AttributeValueUpdate().withAction(AttributeAction.PUT)
	 * 		.withValue(titleValue);
	 * updatePointRequest.getUpdateItemRequest().getAttributeUpdates().put(&quot;title&quot;, titleValueUpdate);
	 * 
	 * UpdatePointResponse updatePointResponse = geoIndexManager.updatePoint(updatePointRequest);
	 * </pre>
	 * 
	 * @param updatePointRequest
	 *            Container for the necessary parameters to execute update point request.
	 * 
	 * @return Response of update point request.
	 */
	public UpdatePointResponse updatePoint(UpdatePointRequest updatePointRequest, Map<String, AttributeValueUpdate> updates) {
		return dynamoDBManager.updatePoint(updatePointRequest, updates);
	}

	/**
	 * <p>
	 * Delete a point from the Amazon DynamoDB table.
	 * </p>
	 * <b>Sample usage:</b>
	 * 
	 * <pre>
	 * GeoPoint geoPoint = new GeoPoint(47.5, -122.3);
	 * 
	 * String rangeKey = &quot;a6feb446-c7f2-4b48-9b3a-0f87744a5047&quot;;
	 * AttributeValue rangeKeyValue = new AttributeValue().withS(rangeKey);
	 * 
	 * DeletePointRequest deletePointRequest = new DeletePointRequest(geoPoint, rangeKeyValue);
	 * DeletePointResponse deletePointResponse = geoIndexManager.deletePoint(deletePointRequest);
	 * </pre>
	 * 
	 * @param deletePointRequest
	 *            Container for the necessary parameters to execute delete point request.
	 * 
	 * @return Response of delete point request.
	 */
	public DeletePointResponse deletePoint(DeletePointRequest deletePointRequest) {
		return dynamoDBManager.deletePoint(deletePointRequest);
	}

	/**
	 * Merge continuous cells in cellUnion and return a list of merged GeohashRanges.
	 * 
	 * @param cellUnion
	 *            Container for multiple cells.
	 * 
	 * @return A list of merged GeohashRanges.
	 */
	private List<GeohashRange> mergeCells(S2CellUnion cellUnion) {

		List<GeohashRange> ranges = new ArrayList<GeohashRange>();
		for (S2CellId c : cellUnion.cellIds()) {
			GeohashRange range = new GeohashRange(c.rangeMin().id(), c.rangeMax().id());

			boolean wasMerged = false;
			for (GeohashRange r : ranges) {
				if (r.tryMerge(range)) {
					wasMerged = true;
					break;
				}
			}

			if (!wasMerged) {
				ranges.add(range);
			}
		}

		return ranges;
	}

	/**
	 * Query Amazon DynamoDB in parallel and filter the result.
	 * 
	 * @param ranges
	 *            A list of geohash ranges that will be used to query Amazon DynamoDB.
	 * 
	 * @param geoQueryRequest
	 *            The rectangle area that will be used as a reference point for precise filtering.
	 * 
	 * @return Aggregated and filtered items returned from Amazon DynamoDB.
	 */
	private GeoQueryResponse dispatchQueries(List<GeohashRange> ranges, GeoQueryRequest geoQueryRequest) {
		GeoQueryResponse geoQueryResponse = new GeoQueryResponse();

		ExecutorService executorService = config.getExecutorService();
		List<Future<?>> futureList = new ArrayList<Future<?>>();

		for (GeohashRange outerRange : ranges) {
			List<GeohashRange> outrRangesSplit = outerRange.trySplit(config.getHashKeyLength());
			for (GeohashRange range : outrRangesSplit) {
				GeoQueryThread geoQueryThread = new GeoQueryThread(geoQueryRequest, geoQueryResponse, range);
				System.out.println("Dispatch Job");
				futureList.add(executorService.submit(geoQueryThread));
			}
		}
		ranges = null;

		for (int i = 0; i < futureList.size(); i++) {
			try {
				futureList.get(i).get();
			} catch (Exception e) {
				for (int j = i + 1; j < futureList.size(); j++) {
					futureList.get(j).cancel(true);
				}
				throw SdkException.builder().cause(e).message("Querying Amazon DynamoDB failed.").build();
			}
		}
		futureList = null;

		return geoQueryResponse;
	}

	/**
	 * Filter out any points outside of the queried area from the input list.
	 * 
	 * @param list
	 *            List of items return by Amazon DynamoDB. It may contains points outside of the actual area queried.
	 * 
	 * @param geoQueryRequest
	 *            Queried area. Any points outside of this area need to be discarded.
	 * 
	 * @return List of items within the queried area.
	 */
	private List<Map<String, AttributeValue>> filter(List<Map<String, AttributeValue>> list,
			GeoQueryRequest geoQueryRequest) {

		List<Map<String, AttributeValue>> result = new ArrayList<Map<String, AttributeValue>>();

		S2LatLngRect latLngRect = null;
		S2LatLng centerLatLng = null;
		double radiusInMeter = 0;
		if (geoQueryRequest instanceof QueryRectangleRequest) {
			latLngRect = S2Util.getBoundingLatLngRect(geoQueryRequest);
		} else if (geoQueryRequest instanceof QueryRadiusRequest) {
			GeoPoint centerPoint = ((QueryRadiusRequest) geoQueryRequest).getCenterPoint();
			centerLatLng = S2LatLng.fromDegrees(centerPoint.getLatitude(), centerPoint.getLongitude());

			radiusInMeter = ((QueryRadiusRequest) geoQueryRequest).getRadiusInMeter();
		}

		for (Map<String, AttributeValue> item : list) {
			String geoJson = item.get(config.getGeoJsonAttributeName()).s();
			GeoPoint geoPoint = GeoJsonMapper.geoPointFromString(geoJson);

			S2LatLng latLng = S2LatLng.fromDegrees(geoPoint.getLatitude(), geoPoint.getLongitude());
			if (latLngRect != null && latLngRect.contains(latLng)) {
				result.add(item);
			} else if (centerLatLng != null && radiusInMeter > 0
					&& S2Util.getEarthDistance(centerLatLng, latLng) <= radiusInMeter) {
				result.add(item);
			}
		}

		return result;
	}

	/**
	 * Worker thread to query Amazon DynamoDB.
	 * */
	private class GeoQueryThread extends Thread {
		private final GeoQueryRequest geoQueryRequest;
		private final GeoQueryResponse geoQueryResponse;
		private final GeohashRange range;

		public GeoQueryThread(GeoQueryRequest geoQueryRequest, GeoQueryResponse geoQueryResponse, GeohashRange range) {
			this.geoQueryRequest = geoQueryRequest;
			this.geoQueryResponse = geoQueryResponse;
			this.range = range;
		}

		public void run() {
			long hashKey = S2Manager.generateHashKey(range.getRangeMin(), config.getHashKeyLength());

			List<QueryResponse> queryResponses = dynamoDBManager.queryGeohash(hashKey, range);

			for (QueryResponse queryResponse : queryResponses) {
				if (isInterrupted()) {
					return;
				}

				// getQueryResponses() returns a synchronized list.
				geoQueryResponse.getQueryResponses().add(queryResponse);

				List<Map<String, AttributeValue>> filteredQueryResponse = filter(queryResponse.items(), geoQueryRequest);

				// getItem() returns a synchronized list.
				geoQueryResponse.getItem().addAll(filteredQueryResponse);
			}
		}
	}
}
