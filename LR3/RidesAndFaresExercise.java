package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class RidesAndFaresExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", pathToRideData);
		final String faresFile = params.get("fares", pathToFareData);

		final int delay = 60;
		final int servingSpeedFactor = 1800;

		StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(
						new TaxiRideSource(ridesFile, delay, servingSpeedFactor)))
				.filter((TaxiRide ride) -> ride.isStart)
				.keyBy("rideId");

		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(
						new TaxiFareSource(faresFile, delay, servingSpeedFactor)))
				.keyBy("rideId");

		DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
				.connect(fares)
				.flatMap(new EnrichmentFunction());

		printOrTest(enrichedRides);

		env.execute("Join Rides with Fares (java RichCoFlatMap)");
	}

	public static class EnrichmentFunction extends
			RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

		// ✅ Состояние: храним поездку пока не придёт тариф
		private ValueState<TaxiRide> rideState;
		// ✅ Состояние: храним тариф пока не придёт поездка
		private ValueState<TaxiFare> fareState;

		@Override
		public void open(Configuration config) throws Exception {
			// Инициализация состояний
			rideState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("saved ride", TaxiRide.class));
			fareState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("saved fare", TaxiFare.class));
		}

		@Override
		public void flatMap1(
				TaxiRide ride,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			TaxiFare fare = fareState.value();
			if (fare != null) {
				// Тариф уже пришёл раньше - очищаем и отдаём пару
				fareState.clear();
				out.collect(new Tuple2<>(ride, fare));
			} else {
				// Тарифа ещё нет - сохраняем поездку и ждём
				rideState.update(ride);
			}
		}

		@Override
		public void flatMap2(
				TaxiFare fare,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			TaxiRide ride = rideState.value();
			if (ride != null) {
				// Поездка уже пришла раньше - очищаем и отдаём пару
				rideState.clear();
				out.collect(new Tuple2<>(ride, fare));
			} else {
				// Поездки ещё нет - сохраняем тариф и ждём
				fareState.update(fare);
			}
		}
	}
}