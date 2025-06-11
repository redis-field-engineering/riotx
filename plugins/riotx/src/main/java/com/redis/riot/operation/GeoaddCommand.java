package com.redis.riot.operation;

import com.redis.batch.operation.Geoadd;
import io.lettuce.core.GeoValue;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

@Command(name = "geoadd", description = "Add members to a geo set")
public class GeoaddCommand extends AbstractMemberOperationCommand {

	@Option(names = "--lon", required = true, description = "Longitude field.", paramLabel = "<field>")
	private String longitude;

	@Option(names = "--lat", required = true, description = "Latitude field.", paramLabel = "<field>")
	private String latitude;

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String field) {
		this.longitude = field;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String field) {
		this.latitude = field;
	}

	@Override
	public Geoadd<byte[], byte[], Map<String, Object>> operation() {
		Function<Map<String, Object>, Collection<byte[]>> members = memberFunction();
		ToDoubleFunction<Map<String, Object>> lon = toDouble(longitude, 0);
		ToDoubleFunction<Map<String, Object>> lat = toDouble(latitude, 0);
		return new Geoadd<>(keyFunction(), t -> value(members, lon, lat, t));
	}

	private Collection<GeoValue<byte[]>> value(Function<Map<String, Object>, Collection<byte[]>> members, ToDoubleFunction<Map<String, Object>> lon, ToDoubleFunction<Map<String, Object>> lat, Map<String, Object> map) {
		Collection<byte[]> ids = members.apply(map);
		double longitude = lon.applyAsDouble(map);
		double latitude = lat.applyAsDouble(map);
		return ids.stream().map(id -> GeoValue.just(longitude, latitude, id)).collect(Collectors.toList());
	}

}
