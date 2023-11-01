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

package com.amazonaws.geo.s2.internal;

import com.amazonaws.geo.model.GeoPoint;
import com.amazonaws.geo.model.GeoQueryRequest;
import com.amazonaws.geo.model.QueryRadiusRequest;
import com.amazonaws.geo.model.QueryRectangleRequest;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

public class S2Util {

	/**
	 * Approximate "effective" radius of the Earth in meters.
	 */
	public static final double EARTH_RADIUS_METERS = 6367000.0;

	/**
	 * Returns the surface distance to the given point assuming the default Earth
	 * radius of {@link #EARTH_RADIUS_METERS}.
	 */
	public static double getEarthDistance(final S2LatLng center, final S2LatLng o) {
		return center.getDistance(o, EARTH_RADIUS_METERS);
	}
	/**
	 * An utility method to get a bounding box of latitude and longitude from a given GeoQueryRequest.
	 * 
	 * @param geoQueryRequest
	 *            It contains all of the necessary information to form a latitude and longitude box.
	 * 
	 * */
	public static S2LatLngRect getBoundingLatLngRect(GeoQueryRequest geoQueryRequest) {
		if (geoQueryRequest instanceof QueryRectangleRequest) {
			QueryRectangleRequest queryRectangleRequest = (QueryRectangleRequest) geoQueryRequest;

			GeoPoint minPoint = queryRectangleRequest.getMinPoint();
			GeoPoint maxPoint = queryRectangleRequest.getMaxPoint();

			S2LatLngRect latLngRect = null;

			if (minPoint != null && maxPoint != null) {
				S2LatLng minLatLng = S2LatLng.fromDegrees(minPoint.getLatitude(), minPoint.getLongitude());
				S2LatLng maxLatLng = S2LatLng.fromDegrees(maxPoint.getLatitude(), maxPoint.getLongitude());

				latLngRect = new S2LatLngRect(minLatLng, maxLatLng);
			}

			return latLngRect;
		} else if (geoQueryRequest instanceof QueryRadiusRequest) {
			QueryRadiusRequest queryRadiusRequest = (QueryRadiusRequest) geoQueryRequest;

			GeoPoint centerPoint = queryRadiusRequest.getCenterPoint();
			double radiusInMeter = queryRadiusRequest.getRadiusInMeter();

			S2LatLng centerLatLng = S2LatLng.fromDegrees(centerPoint.getLatitude(), centerPoint.getLongitude());

			double latReferenceUnit = centerPoint.getLatitude() > 0.0 ? -1.0 : 1.0;
			S2LatLng latReferenceLatLng = S2LatLng.fromDegrees(centerPoint.getLatitude() + latReferenceUnit,
					centerPoint.getLongitude());
			double lngReferenceUnit = centerPoint.getLongitude() > 0.0 ? -1.0 : 1.0;
			S2LatLng lngReferenceLatLng = S2LatLng.fromDegrees(centerPoint.getLatitude(), centerPoint.getLongitude()
					+ lngReferenceUnit);

			double latForRadius = radiusInMeter / S2Util.getEarthDistance(centerLatLng, latReferenceLatLng);
			double lngForRadius = radiusInMeter / S2Util.getEarthDistance(centerLatLng, lngReferenceLatLng);

			S2LatLng minLatLng = S2LatLng.fromDegrees(centerPoint.getLatitude() - latForRadius,
					centerPoint.getLongitude() - lngForRadius);
			S2LatLng maxLatLng = S2LatLng.fromDegrees(centerPoint.getLatitude() + latForRadius,
					centerPoint.getLongitude() + lngForRadius);

			return new S2LatLngRect(minLatLng, maxLatLng);
		}

		return null;
	}

}
