package com.example.kafka.streams;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.json.JSONException;
import org.json.JSONObject;

@SpringBootApplication
public class App {

	private static final String initialTopic = "GithubEvents";
	private static final String filterTopic = "GithubPushEvents";
	private static final String aggregationTopic = "GithubEventAggregates";

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
				LogAndContinueExceptionHandler.class);
		props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
				DefaultProductionExceptionHandler.class);
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

		StreamsBuilder builder = new StreamsBuilder();

		builder.stream(initialTopic, Consumed.with(Serdes.String(), Serdes.String()))
				.filter((key, value) -> checkGitHubEventType(value)).map((key, value) -> toGitHubPushEvent(key, value))
				.to(filterTopic, Produced.with(Serdes.String(), Serdes.String()));

		builder.stream(filterTopic, Consumed.with(Serdes.String(), Serdes.String())).groupByKey()
				.windowedBy(TimeWindows.of(Duration.ofMinutes(1))).count().toStream()
				.map((Windowed<String> key, Long count) -> aggregate(key, count))
				.to(aggregationTopic, Produced.with(Serdes.String(), Serdes.String()));

		Topology topology = builder.build();
		KafkaStreams streams = new KafkaStreams(topology, props);
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	private static boolean checkGitHubEventType(String value) {
		try {
			JSONObject jsonObject = new JSONObject(value);
			if (jsonObject.getString("type").equals("PushEvent")) {
				return true;
			}
			return false;
		} catch (JSONException e) {
			e.printStackTrace();
			return false;
		}
	}

	private static KeyValue<String, String> toGitHubPushEvent(String key, String value) {
		try {
			JSONObject jsonObject = new JSONObject(value);
			jsonObject.remove("type");
			String s = jsonObject.toString();
			return new KeyValue<String, String>(key, s);
		} catch (JSONException e) {
			e.printStackTrace();
			return new KeyValue<String, String>(key, value);
		}
	}

	private static KeyValue<String, String> aggregate(Windowed<String> key, Long count) {
		try {
			GithubEventAggregate ag = new GithubEventAggregate("PushEvent", key.window().startTime().toString(),
					key.window().endTime().toString(), count,
					new SimpleDateFormat("YYYY-MM-DDTHH:mm:ss").format(Calendar.getInstance().getTime()));
			JSONObject res = new JSONObject(ag);
			return new KeyValue<String, String>(key.key(), res.toString());
		} catch (JSONException e) {
			e.printStackTrace();
			return new KeyValue<String, String>(key.key(), e.toString());
		}
	}
}
